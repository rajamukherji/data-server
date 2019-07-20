#include "minilang/minilang.h"
#include "minilang/ml_console.h"
#include "minilang/ml_macros.h"
#include "minilang/ml_file.h"
#include "minilang/ml_object.h"
#include "minilang/ml_iterfns.h"
#include "minilang/stringmap.h"
#include <stdio.h>
#include <stdlib.h>
#include <czmq.h>
#include <gc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <jansson.h>
#include "dataset.h"

static stringmap_t Datasets[1] = {STRINGMAP_INIT};
static const char *DatasetsPath = 0;

static void datasets_load() {
	DIR *Dir = opendir(DatasetsPath);
	if (!Dir) {
		fprintf(stderr, "Error opening dataset path: %s\n", DatasetsPath);
		exit(-1);
	}
	struct dirent *Entry;
	while ((Entry = readdir(Dir))) {
		if (Entry->d_name[0] != '.' && Entry->d_type == DT_DIR) {
			char *Path;
			asprintf(&Path, "%s/%s", DatasetsPath, Entry->d_name);
			dataset_t *Dataset = dataset_open(Path);
			if (Dataset) stringmap_insert(Datasets, GC_strdup(Entry->d_name), Dataset);
		}
	}
	closedir(Dir);
}

typedef struct client_t {
	const char *Id;
	zframe_t *Frame;
	dataset_t *Dataset;
} client_t;

static stringmap_t Clients[1] = {STRINGMAP_INIT};
static stringmap_t Methods[1] = {STRINGMAP_INIT};
static zsock_t *Socket;

static void datasets_serve(int Port) {
	static char HexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
	Socket = zsock_new_router(NULL);
	zsock_bind(Socket, "tcp://*:%d", Port);
	for (;;) {
		zmsg_t *RequestMsg = zmsg_recv(Socket);
		zmsg_print(RequestMsg);
		zframe_t *ClientFrame = zmsg_pop(RequestMsg);
		size_t IdSize = zframe_size(ClientFrame);
		byte *IdData = zframe_data(ClientFrame);
		char ClientId[2 * IdSize + 1];
		char *Id = ClientId;
		for (int I = 0; I < IdSize; ++I) {
			Id[2 * I] = HexDigits[IdData[I] & 15];
			Id[2 * I + 1] = HexDigits[(IdData[I] / 16) & 15];
		}
		Id[2 * IdSize] = 0;
		client_t *Client = stringmap_search(Clients, ClientId);
		if (!Client) {
			Client = new(client_t);
			Client->Id = GC_strdup(ClientId);
			stringmap_insert(Clients, Client->Id, Client);
			Client->Frame = zframe_dup(ClientFrame);
		}
		zframe_t *RequestFrame = zmsg_pop(RequestMsg);
		json_error_t Error;
		json_t *Request = json_loadb(zframe_data(RequestFrame), zframe_size(RequestFrame), 0, &Error);
		if (!Request) {
			fprintf(stderr, "Error: %s:%d: %s\n", Error.source, Error.line, Error.text);
			continue;
		}
		int Index;
		const char *Method;
		json_t *Argument;
		if (json_unpack(Request, "[iso]", &Index, &Method, &Argument)) {
			fprintf(stderr, "Error: invalid request\n");
			continue;
		}
		printf("Method = %s\n", Method);
		json_t *(*MethodFn)(client_t *, json_t *) = stringmap_search(Methods, Method);
		json_t *Result;
		if (MethodFn) {
			Result = MethodFn(Client, Argument);
		} else {
			fprintf(stderr, "Error: unknown method %s\n", Method);
			Result = json_pack("{ss}", "error", "invalid method");
		}
		zmsg_t *ResponseMsg = zmsg_new();
		zmsg_append(ResponseMsg, &ClientFrame);
		json_t *Response = json_pack("[io]", Index, Result);
		zmsg_addstr(ResponseMsg, json_dumps(Response, JSON_COMPACT));
		zmsg_print(ResponseMsg);
		zmsg_send(&ResponseMsg, Socket);
	}
}

static void client_alert(client_t *Client, json_t *Alert) {
	zmsg_t *Msg = zmsg_new();
	zframe_t *ClientFrame = zframe_dup(Client->Frame);
	zmsg_append(Msg, &ClientFrame);
	zmsg_addstr(Msg, json_dumps(json_pack("[io]", 0, Alert), JSON_COMPACT));
	zmsg_print(Msg);
	zmsg_send(&Msg, Socket);
}

static int dataset_list_fn(const char *Id, dataset_t *Dataset, json_t *Result) {
	json_array_append(Result, json_pack("{sssO}", "id", Id, "info", dataset_get_info(Dataset)));
	return 0;
}

static json_t *method_dataset_list(client_t *Client, json_t *Argument) {
	json_t *Result = json_array();
	stringmap_foreach(Datasets, Result, (void *)dataset_list_fn);
	return Result;
}

static json_t *method_dataset_create(client_t *Client, json_t *Argument) {
	const char *Name;
	int Length;
	if (json_unpack(Argument, "{sssi}", "name", &Name, "length", &Length)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	char *Path;
	int PathLength = asprintf(&Path, "%s/XXXXXX", DatasetsPath);
	mkdtemp(Path);
	char *Id = Path + PathLength - 6;
	dataset_t *Dataset = dataset_create(Path, Name, Length);
	if (!Dataset) return json_pack("{ss}", "error", "failed to create dataset");
	stringmap_insert(Datasets, Id, Dataset);
	Client->Dataset = Dataset;
	dataset_watcher_add(Dataset, Client->Id, Client);
	return dataset_get_info(Dataset);
}

static json_t *method_dataset_open(client_t *Client, json_t *Argument) {
	const char *Id;
	if (json_unpack(Argument, "{ss}", "id", &Id)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	dataset_t *Dataset = stringmap_search(Datasets, Id);
	if (!Dataset) return json_pack("{ss}", "errror", "invalid id");
	Client->Dataset = Dataset;
	dataset_watcher_add(Dataset, Client->Id, Client);
	return dataset_get_info(Dataset);
}

static json_t *method_dataset_close(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	dataset_watcher_remove(Client->Dataset, Client->Id);
	Client->Dataset = NULL;
	return json_null();
}

static json_t *method_dataset_info(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	return dataset_get_info(Client->Dataset);
}

typedef struct alert_column_create_t {
	client_t *Cause;
	dataset_t *Dataset;
	column_t *Column;
} alert_column_create_t;

static int alert_column_create(const char *Id, client_t *Client, alert_column_create_t *Alert) {
	if (Client != Alert->Cause && Client->Dataset == Alert->Dataset) {
		client_alert(Client, json_pack("[ssO]",
			"column/created",
			column_get_id(Alert->Column),
			dataset_get_column_info(Alert->Dataset, column_get_id(Alert->Column))
		));
	}
	return 0;
}

static json_t *method_column_create(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	const char *Name, *TypeString;
	if (json_unpack(Argument, "{ssss}", "name", &Name, "type", &TypeString)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	column_type_t Type;
	if (!strcmp(TypeString, "string")) {
		Type = COLUMN_STRING;
	} else if (!strcmp(TypeString, "real")) {
		Type = COLUMN_REAL;
	} else {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	column_t *Column = dataset_column_create(Client->Dataset, Name, Type);
	if (!Column) if (!Column) return json_pack("{ss}", "error", "failed to create column");
	column_watcher_add(Column, Client->Id, Client);
	alert_column_create_t Alert[1] = {{Client, Client->Dataset, Column}};
	dataset_watcher_foreach(Client->Dataset, Alert, (void *)alert_column_create);
	return json_string(column_get_id(Column));
}

static json_t *method_column_open(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	const char *Id;
	if (json_unpack(Argument, "{ss}", "column", &Id)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	column_t *Column = dataset_column_open(Client->Dataset, Id);
	if (!Column) return json_pack("{ss}", "error", "invalid id");
	column_watcher_add(Column, Client->Id, Client);
	return json_pack("{ss}", "id", column_get_id(Column));
}

static json_t *method_column_close(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	const char *Id;
	if (json_unpack(Argument, "{ss}", "column", &Id)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	column_t *Column = dataset_column_open(Client->Dataset, Id);
	if (!Column) return json_pack("{ss}", "error", "invalid id");
	column_watcher_remove(Column, Client->Id);
	return json_null();
}

typedef struct alert_column_values_set_t {
	client_t *Cause;
	dataset_t *Dataset;
	column_t *Column;
	json_t *Indices, *Values;
	json_int_t Generation;
} alert_column_values_set_t;

static int alert_column_values_set(const char *Id, client_t *Client, alert_column_values_set_t *Alert) {
	if (Client != Alert->Cause && Client->Dataset == Alert->Dataset) {
		client_alert(Client, json_pack("[ssiOO]",
			"column/values/set",
			column_get_id(Alert->Column),
			Alert->Generation,
			Alert->Indices, Alert->Values
		));
	}
	return 0;
}

static json_t *method_column_values_set(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	const char *Id;
	json_t *Values;
	if (json_unpack(Argument, "{ssso}", "column", &Id, "values", &Values)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	if (!json_is_array(Values)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	column_t *Column = dataset_column_open(Client->Dataset, Id);
	if (!Column) return json_pack("{ss}", "error", "invalid id");
	json_t *Indices = json_object_get(Argument, "indices");
	if (json_is_array(Indices)) {
		size_t Count = json_array_size(Indices);
		if (json_array_size(Values) != Count) return json_pack("{ss}", "error", "invalid arguments");
		switch (column_get_type(Column)) {
		case COLUMN_REAL: {
			for (size_t I = 0; I < Count; ++I) {
				int Index = json_integer_value(json_array_get(Indices, I));
				double Value = json_number_value(json_array_get(Values, I));
				column_real_set(Column, Index, Value);
			}
			break;
		}
		case COLUMN_STRING: {
			int Required = 0;
			for (size_t I = 0; I < Count; ++I) {
				int Index = json_integer_value(json_array_get(Indices, I));
				json_t *String = json_array_get(Values, I);
				int Length = json_string_length(String);
				Required += column_string_extend_hint(Column, Index, Length);
			}
			column_string_extend(Column, Required);
			for (size_t I = 0; I < Count; ++I) {
				int Index = json_integer_value(json_array_get(Indices, I));
				json_t *String = json_array_get(Values, I);
				const char *Value = json_string_value(String);
				int Length = json_string_length(String);
				column_string_set(Column, Index, Value, Length);
			}
			break;
		}
		}
		alert_column_values_set_t Alert[1] = {{
			Client, Client->Dataset, Column,
			Indices, Values, column_generation_bump(Column)
		}};
		column_watcher_foreach(Column, Alert, (void *)alert_column_values_set);
		return json_integer(Alert->Generation);
	} else {
		size_t Length = json_array_size(Values);
		if (Length != column_get_length(Column)) return json_pack("{ss}", "error", "invalid arguments");
		switch (column_get_type(Column)) {
		case COLUMN_REAL: {
			for (size_t Index = 0; Index < Length; ++Index) {
				double Value = json_number_value(json_array_get(Values, Index));
				column_real_set(Column, Index, Value);
			}
			break;
		}
		case COLUMN_STRING: {
			int Required = 0;
			for (size_t Index = 0; Index < Length; ++Index) {
				json_t *String = json_array_get(Values, Index);
				int Length = json_string_length(String);
				Required += column_string_extend_hint(Column, Index, Length);
			}
			column_string_extend(Column, Required);
			for (size_t Index = 0; Index < Length; ++Index) {
				json_t *String = json_array_get(Values, Index);
				const char *Value = json_string_value(String);
				int Length = json_string_length(String);
				column_string_set(Column, Index, Value, Length);
			}
			break;
		}
		}
		return json_null();
	}
}

static json_t *method_column_values_get(client_t *Client, json_t *Argument) {
	if (!Client->Dataset) return json_pack("{ss}", "error", "no dataset open");
	const char *Id;
	if (json_unpack(Argument, "{ss}", "column", &Id)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	column_t *Column = dataset_column_open(Client->Dataset, Id);
	if (!Column) return json_pack("{ss}", "error", "invalid id");
	json_t *Values = json_array();
	json_t *Indices = json_object_get(Argument, "indices");
	if (json_is_array(Indices)) {
		size_t Count = json_array_size(Indices);
		switch (column_get_type(Column)) {
		case COLUMN_REAL: {
			for (size_t I = 0; I < Count; ++I) {
				int Index = json_integer_value(json_array_get(Indices, I));
				json_array_append(Values, json_real(column_real_get(Column, Index)));
			}
			break;
		}
		case COLUMN_STRING: {
			for (size_t I = 0; I < Count; ++I) {
				int Index = json_integer_value(json_array_get(Indices, I));
				int Length = column_string_get_length(Column, Index);
				char *Value = GC_malloc_atomic(Length + 1);
				column_string_get_value(Column, Index, Value);
				Value[Length] = 0;
				json_array_append(Values, json_string(Value));
			}
			break;
		}
		}
	} else {
		size_t Length = dataset_get_length(Client->Dataset);
		switch (column_get_type(Column)) {
		case COLUMN_REAL: {
			for (size_t Index = 0; Index < Length; ++Index) {
				json_array_append(Values, json_real(column_real_get(Column, Index)));
			}
			break;
		}
		case COLUMN_STRING: {
			for (size_t Index = 0; Index < Length; ++Index) {
				int Length = column_string_get_length(Column, Index);
				char *Value = GC_malloc_atomic(Length + 1);
				column_string_get_value(Column, Index, Value);
				Value[Length] = 0;
				json_array_append(Values, json_string(Value));
			}
			break;
		}
		}
	}
	return Values;
}

static stringmap_t Globals[1] = {STRINGMAP_INIT};

static ml_value_t *global_get(void *Data, const char *Name) {
	return stringmap_search(Globals, Name) ?: MLNil;
}

static ml_value_t *print(void *Data, int Count, ml_value_t **Args) {
	static ml_value_t *StringMethod = 0;
	if (!StringMethod) StringMethod = ml_method("string");
	for (int I = 0; I < Count; ++I) {
		ml_value_t *Result = Args[I];
		if (Result->Type != MLStringT) {
			Result = ml_call(StringMethod, 1, &Result);
			if (Result->Type == MLErrorT) return Result;
			if (Result->Type != MLStringT) return ml_error("ResultError", "string method did not return string");
		}
		fwrite(ml_string_value(Result), 1, ml_string_length(Result), stdout);
	}
	fflush(stdout);
	return MLNil;
}

static ml_value_t *error(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(2);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ML_CHECK_ARG_TYPE(1, MLStringT);
	return ml_error(ml_string_value(Args[0]), "%s", ml_string_value(Args[1]));
}

int main(int Argc, char **Argv) {
	ml_init();
	ml_file_init(Globals);
	ml_object_init(Globals);
	ml_iterfns_init(Globals);
	stringmap_insert(Globals, "print", ml_function(0, print));
	stringmap_insert(Globals, "error", ml_function(0, error));
	dataset_init(Globals);

	int Port = 9001;
	for (int I = 1; I < Argc; ++I) {
		if (Argv[I][0] == '-') {
			if (Argv[I][1] == 'p') {
				if (Argv[I][2]) {
					Port = atoi(Argv[I] + 2);
				} else {
					Port = atoi(Argv[++I]);
				}
			}
		} else {
			DatasetsPath = Argv[I];
		}
	}
	if (DatasetsPath) {
		stringmap_insert(Methods, "dataset/list", method_dataset_list);
		stringmap_insert(Methods, "dataset/create", method_dataset_create);
		stringmap_insert(Methods, "dataset/open", method_dataset_open);
		stringmap_insert(Methods, "dataset/close", method_dataset_close);
		stringmap_insert(Methods, "dataset/info", method_dataset_info);
		stringmap_insert(Methods, "column/create", method_column_create);
		stringmap_insert(Methods, "column/open", method_column_open);
		stringmap_insert(Methods, "column/close", method_column_close);
		stringmap_insert(Methods, "column/values/set", method_column_values_set);
		stringmap_insert(Methods, "column/values/get", method_column_values_get);
		datasets_load();
		datasets_serve(Port);
	}

	ml_console(global_get, Globals);
	return 0;
}
