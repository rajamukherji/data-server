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
#include <jansson.h>
#include "dataset.h"

typedef struct dataset_entry_t dataset_entry_t;

struct dataset_entry_t {
	dataset_entry_t *Next;
	dataset_t *Dataset;
};

static dataset_entry_t *DatasetEntries = 0;
static const char *DatasetPath = 0;

static void datasets_load() {
	dataset_entry_t **Slot = &DatasetEntries;
	char DatasetPath[strlen(DatasetPath) + 10];
	for (int Index = 0; ; ++Index) {
		struct stat Stat[1];
		sprintf(DatasetPath, "%s/%d", DatasetPath, Index);
		if (stat(DatasetPath, Stat)) break;
		if (!S_ISDIR(Stat->st_mode)) break;
		dataset_entry_t *Entry = Slot[0] = new(dataset_entry_t);
		Entry->Dataset = dataset_open(GC_strdup(DatasetPath));
		Slot = &Entry->Next;
	}
}

typedef struct client_t {
	zframe_t *Frame;
	dataset_t *Dataset;
} client_t;

static stringmap_t Clients[1] = {STRINGMAP_INIT};
static stringmap_t Methods[1] = {STRINGMAP_INIT};

static void datasets_serve(int Port) {
	static char HexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
	zsock_t *Socket = zsock_new_router(NULL);
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
			stringmap_insert(Clients, GC_strdup(ClientId), Client);
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
		if (!MethodFn) {
			fprintf(stderr, "Error: unknown method %s\n", Method);
			continue;
		}
		json_t *Result = MethodFn(Client, Argument);
		zmsg_t *ResponseMsg = zmsg_new();
		zmsg_append(ResponseMsg, &ClientFrame);
		json_t *Response = json_pack("[io]", Index, Result);
		zmsg_addstr(ResponseMsg, json_dumps(Response, JSON_COMPACT));
		zmsg_print(ResponseMsg);
		zmsg_send(&ResponseMsg, Socket);
	}
}

static json_t *method_dataset_list(client_t *Client, json_t *Argument) {
	json_t *Result = json_array();
	int Index = 0;
	for (dataset_entry_t *Entry = DatasetEntries; Entry; Entry = Entry->Next) {
		json_array_append(Result, json_pack("{sisO}", "index", Index, "info", dataset_get_info(Entry->Dataset)));
		++Index;
	}
	return Result;
}

static json_t *method_dataset_create(client_t *Client, json_t *Argument) {
	const char *Name;
	int Length;
	if (json_unpack(Argument, "{sssi}", "name", &Name, "length", &Length)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	dataset_entry_t **Slot = &DatasetEntries;
	int Index = 0;
	while (Slot[0]) {
		Slot = &Slot[0]->Next;
		++Index;
	}
	char *Path;
	asprintf(&Path, "%s/%d", DatasetPath, Index);
	dataset_entry_t *Entry = Slot[0] = new(dataset_entry_t);
	Client->Dataset = Entry->Dataset = dataset_create(Path, Name, Length);
	return json_pack("{sisO}", "index", Index, "info", dataset_get_info(Entry->Dataset));
}

static json_t *method_dataset_open(client_t *Client, json_t *Argument) {
	int Index;
	if (json_unpack(Argument, "{si}", "index", &Index)) {
		return json_pack("{ss}", "error", "invalid arguments");
	}
	dataset_entry_t *Entry = DatasetEntries;
	for (int I = 0; I < Index; ++I) {
		if (!Entry) return json_pack("{ss}", "errror", "invalid index");
		Entry = Entry->Next;
	}
	if (!Entry) return json_pack("{ss}", "errror", "invalid index");
	Client->Dataset = Entry->Dataset;
	return json_pack("{sisO}", "index", Index, "info", dataset_get_info(Entry->Dataset));
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
			DatasetPath = Argv[I];
		}
	}
	if (DatasetPath) {
		stringmap_insert(Methods, "dataset/list", method_dataset_list);
		stringmap_insert(Methods, "dataset/create", method_dataset_create);
		stringmap_insert(Methods, "dataset/open", method_dataset_open);
		datasets_load();
		datasets_serve(Port);
	}

	ml_console(global_get, Globals);
	return 0;
}
