#include "dataset.h"
#include "minilang/minilang.h"
#include "minilang/ml_macros.h"
#include <gc.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

/*
string column structure (with N values):
	header
	entry * N
	nodes * header->count
*/

typedef struct string_header_t {
	int32_t FreeStart, FreeCount;
} string_header_t;

typedef struct string_entry_t {
	int32_t Link, Length;
} string_entry_t;

typedef union string_node_t {
	struct { int32_t Link; char Small[12]; };
	char Large[16];
} string_node_t;

typedef struct column_callback_node_t column_callback_node_t;

struct column_callback_node_t {
	column_callback_node_t *Next;
	column_callback_t Callback;
	void *Data;
};

struct column_t {
	const ml_type_t *Type;
	column_t *Next;
	dataset_t *Dataset;
	const char *Name, *Id;
	union {
		void *Map;
		struct {
			string_header_t Header;
			string_entry_t Entries[];
		} *Strings;
		double *Reals;
	};
	column_callback_node_t *Callbacks;
	size_t MapSize;
	column_type_t DataType;
	int Fd;
};

struct dataset_t {
	const ml_type_t *Type;
	const char *Path, *Name, *InfoFile;
	json_t *Info;
	stringmap_t Columns[1];
	size_t Length;
};

static ml_type_t *DatasetT;
static ml_type_t *ColumnT;

column_type_t column_get_type(column_t *Column) {
	return Column->DataType;
}

const char *column_get_id(column_t *Column) {
	return Column->Id;
}

size_t column_get_length(column_t *Column) {
	return Column->Dataset->Length;
}

static void column_open(column_t *Column) {
	char FileName[strlen(Column->Dataset->Path) + 10];
	sprintf(FileName, "%s/%s", Column->Dataset->Path, Column->Id);
	struct stat Stat[1];
	if (stat(FileName, Stat)) return;
	Column->Fd = open(FileName, O_RDWR, 0777);
	Column->MapSize = Stat->st_size;
	Column->Map = mmap(NULL, Column->MapSize, PROT_READ | PROT_WRITE, MAP_SHARED, Column->Fd, 0);
}

size_t column_string_get_length(column_t *Column, size_t Index) {
	if (!Column->Map) column_open(Column);
	if (Index >= Column->Dataset->Length) return 0;
	return Column->Strings->Entries[Index].Length;
}

void column_string_get_value(column_t *Column, size_t Index, char *Buffer) {
	if (!Column->Map) column_open(Column);
	if (Index >= Column->Dataset->Length) return;
	string_node_t *Nodes = (string_node_t *)(Column->Strings->Entries + Column->Dataset->Length);
	int32_t Length = Column->Strings->Entries[Index].Length;
	int32_t Link = Column->Strings->Entries[Index].Link;
	string_node_t *Node = Nodes + Link;
	while (Length > 16) {
		memcpy(Buffer, Node->Small, 12);
		Buffer += 12;
		Length -= 12;
		Node = Nodes + Node->Link;
	}
	memcpy(Buffer, Node->Large, Length);
}

void column_string_set(column_t *Column, size_t Index, const char *Value, int Length) {
	if (!Column->Map) column_open(Column);
	if (Index >= Column->Dataset->Length) return;
	string_node_t *Nodes = (string_node_t *)(Column->Strings->Entries + Column->Dataset->Length);
	int OldLength = Column->Strings->Entries[Index].Length;
	Column->Strings->Entries[Index].Length = Length;
	int NumBlocksOld = 1 + (OldLength - 5) / 12;
	if (NumBlocksOld < 1) NumBlocksOld = 1;
	int NumBlocksNew = 1 + (Length - 5) / 12;
	if (NumBlocksNew < 1) NumBlocksNew = 1;
	if (NumBlocksOld > NumBlocksNew) {
		string_node_t *Node = Nodes + Column->Strings->Entries[Index].Link;
		while (Length > 16) {
			memcpy(Node->Small, Value, 12);
			Value += 12;
			Length -= 12;
			Node = Nodes + Node->Link;
		}
		int32_t FreeStart = Node->Link;
		string_node_t *FreeEnd = Nodes + FreeStart;
		memcpy(Node->Large, Value, Length);
		int FreeCount = NumBlocksOld - NumBlocksNew;
		Column->Strings->Header.FreeCount += FreeCount;
		while (--FreeCount > 0) FreeEnd = Nodes + FreeEnd->Link;
		FreeEnd->Link = Column->Strings->Header.FreeStart;
		Column->Strings->Header.FreeStart = FreeStart;
	} else if (NumBlocksOld < NumBlocksNew) {
		int UseCount = NumBlocksNew - NumBlocksOld;
		int FreeCount = Column->Strings->Header.FreeCount;
		if (UseCount > FreeCount) {
			int Shortfall = UseCount - FreeCount;
			size_t MapSize = Column->MapSize + Shortfall * sizeof(string_node_t);
			msync(Column->Map, Column->MapSize, MS_SYNC);
			ftruncate(Column->Fd, MapSize);
			Column->Map = mremap(Column->Map, Column->MapSize, MapSize, MREMAP_MAYMOVE);
			Nodes = (string_node_t *)(Column->Strings->Entries + Column->Dataset->Length);
			int32_t FreeEnd;
			if (FreeCount > 0) {
				FreeEnd = Column->Strings->Header.FreeStart;
				while (--FreeCount > 0) FreeEnd = Nodes[FreeEnd].Link;
				FreeEnd = Nodes[FreeEnd].Link = (Column->MapSize - sizeof(string_header_t) - Column->Dataset->Length * sizeof(string_entry_t)) / sizeof(string_node_t);
			} else {
				FreeEnd = (Column->MapSize - sizeof(string_header_t) - Column->Dataset->Length * sizeof(string_entry_t)) / sizeof(string_node_t);
				Column->Strings->Header.FreeStart = FreeEnd;
			}
			while (--Shortfall > 0) FreeEnd = Nodes[FreeEnd].Link = FreeEnd + 1;
			Column->MapSize = MapSize;
			Column->Strings->Header.FreeCount = 0;
		} else {
			Column->Strings->Header.FreeCount -= UseCount;
		}
		string_node_t *Node = Nodes + Column->Strings->Entries[Index].Link;
		while (--NumBlocksOld > 0) {
			memcpy(Node->Small, Value, 12);
			Value += 12;
			Length -= 12;
			Node = Nodes + Node->Link;
		}
		Node->Link = Column->Strings->Header.FreeStart;
		while (Length > 16) {
			memcpy(Node->Small, Value, 12);
			Value += 12;
			Length -= 12;
			Node = Nodes + Node->Link;
		}
		Column->Strings->Header.FreeStart = Node->Link;
		memcpy(Node->Large, Value, Length);
	} else {
		string_node_t *Node = Nodes + Column->Strings->Entries[Index].Link;
		while (Length > 16) {
			memcpy(Node->Small, Value, 12);
			Value += 12;
			Length -= 12;
			Node = Nodes + Node->Link;
		}
		memcpy(Node->Large, Value, Length);
	}
	msync(Column->Map, Column->MapSize, MS_ASYNC);
	for (column_callback_node_t *Node = Column->Callbacks; Node; Node = Node->Next) {
		Node->Callback(Column, Index, Node->Data);
	}
}

double column_real_get(column_t *Column, size_t Index) {
	if (!Column->Map) column_open(Column);
	return Column->Reals[Index];
}

void column_real_set(column_t *Column, size_t Index, double Value) {
	if (!Column->Map) column_open(Column);
	Column->Reals[Index] = Value;
	msync(Column->Map, Column->MapSize, MS_ASYNC);
	for (column_callback_node_t *Node = Column->Callbacks; Node; Node = Node->Next) {
		Node->Callback(Column, Index, Node->Data);
	}
}

void column_watcher_add(column_t *Column, void *Data, column_callback_t Callback) {
	column_callback_node_t *Node = new(column_callback_node_t);
	Node->Callback = Callback;
	Node->Data = Data;
	Node->Next = Column->Callbacks;
	Column->Callbacks = Node;
}

void column_watcher_remove(column_t *Column, void *Data) {
	column_callback_node_t **Slot = &Column->Callbacks;
	while (Slot[0]) {
		if (Slot[0]->Data == Data) {
			Slot[0] = Slot[0]->Next;
			return;
		}
		Slot = &Slot[0]->Next;
	}
}

dataset_t *dataset_create(const char *Path, const char *Name, size_t Length) {
	printf("Creating dataset: %s with %d entries at %s\n", Name, Length, Path);
	dataset_t *Dataset = new(dataset_t);
	Dataset->Type = DatasetT;
	Dataset->Path = Path;
	Dataset->Name = Name;
	Dataset->Length = Length;
	asprintf((char **)&Dataset->InfoFile, "%s/info.json", Path);
	Dataset->Info = json_pack("{sssis{}}", "name", Name, "length", Length, "columns");
	dataset_column_create(Dataset, "image", COLUMN_STRING);
	return Dataset;
}

dataset_t *dataset_open(const char *Path) {
	dataset_t *Dataset = new(dataset_t);
	Dataset->Type = DatasetT;
	Dataset->Path = Path;
	asprintf((char **)&Dataset->InfoFile, "%s/info.json", Path);
	json_error_t Error;
	Dataset->Info = json_load_file(Dataset->InfoFile, 0, &Error);
	if (!Dataset->Info) {
		fprintf(stderr, "Error: %s:%d: %s\n", Error.source, Error.line, Error.text);
		return NULL;
	}
	json_t *ColumnsJson;
	json_unpack(Dataset->Info, "{sssiso}", "name", &Dataset->Name, "length", &Dataset->Length, "columns", &ColumnsJson);
	json_t *ColumnJson;
	const char *Id;
	json_object_foreach(ColumnsJson, Id, ColumnJson) {
		column_t *Column = new(column_t);
		Column->Type = ColumnT;
		Column->Dataset = Dataset;
		Column->Id = Id;
		json_unpack(ColumnJson, "{sssi}", "name", &Column->Name, "type", &Column->DataType);
		stringmap_insert(Dataset->Columns, Id, Column);
	}
	return Dataset;
}

json_t *dataset_get_info(dataset_t *Dataset) {
	return Dataset->Info;
}

size_t dataset_get_length(dataset_t *Dataset) {
	return Dataset->Length;
}

size_t dataset_get_column_count(dataset_t *Dataset) {
	return Dataset->Columns->Size;
}

column_type_t dataset_get_column_type(dataset_t *Dataset, const char *Id) {
	column_t *Column = stringmap_search(Dataset->Columns, Id);
	if (!Column) return 0;
	return Column->DataType;
}

const char *dataset_get_column_name(dataset_t *Dataset, const char *Id) {
	column_t *Column = stringmap_search(Dataset->Columns, Id);
	if (!Column) return NULL;
	return Column->Name;
}

column_t *dataset_column_create(dataset_t *Dataset, const char *Name, column_type_t Type) {
	column_t *Column = new(column_t);
	Column->Type = ColumnT;
	Column->Dataset = Dataset;
	char FileName[strlen(Dataset->Path) + 10];
	char *Id = stpcpy(FileName, Dataset->Path);
	strcpy(Id, "/XXXXXX");
	++Id;
	Column->Fd = mkstemp(FileName);
	Column->Id = GC_strdup(Id);
	Column->Name = Name;
	Column->DataType = Type;
	switch (Type) {
	case COLUMN_STRING: {
		Column->MapSize = sizeof(string_header_t) + Dataset->Length * sizeof(string_entry_t) + Dataset->Length * sizeof(string_node_t);
		ftruncate(Column->Fd, Column->MapSize);
		Column->Map = mmap(NULL, Column->MapSize, PROT_READ | PROT_WRITE, MAP_SHARED, Column->Fd, 0);
		Column->Strings->Header.FreeCount = 0;
		Column->Strings->Header.FreeStart = 0;
		for (int I = 0; I < Dataset->Length; ++I) Column->Strings->Entries[I].Link = I;
		break;
	}
	case COLUMN_REAL: {
		Column->MapSize = Dataset->Length * sizeof(double);
		ftruncate(Column->Fd, Column->MapSize);
		Column->Map = mmap(NULL, Column->MapSize, PROT_READ | PROT_WRITE, MAP_SHARED, Column->Fd, 0);
		break;
	}
	}
	stringmap_insert(Dataset->Columns, Column->Id, Column);
	json_t *ColumnsJson = json_object_get(Dataset->Info, "columns");
	json_object_set(ColumnsJson, Column->Id, json_pack("{sssi}", "name", Name, "type", Type));
	json_dump_file(Dataset->Info, Dataset->InfoFile, 0);
	msync(Column->Map, Column->MapSize, MS_ASYNC);
	return Column;
}

column_t *dataset_column_open(dataset_t *Dataset, const char *Id) {
	column_t *Column = stringmap_search(Dataset->Columns, Id);
	if (!Column) return NULL;
	if (!Column->Map) column_open(Column);
	return Column;
}

static ml_value_t *ml_dataset_open(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(1);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	dataset_t *Dataset = dataset_open(ml_string_value(Args[0]));
	if (Dataset) {
		return (ml_value_t *)Dataset;
	} else {
		return ml_error("LoadError", "Error opening dataset");
	}
}

static ml_value_t *ml_dataset_create(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(3);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ML_CHECK_ARG_TYPE(1, MLStringT);
	ML_CHECK_ARG_TYPE(2, MLIntegerT);
	dataset_t *Dataset = dataset_create(ml_string_value(Args[0]), ml_string_value(Args[1]), ml_integer_value(Args[2]));
	if (Dataset) {
		return (ml_value_t *)Dataset;
	} else {
		return ml_error("LoadError", "Error opening dataset");
	}
}

static ml_value_t *ml_dataset_column_count(void *Data, int Count, ml_value_t **Args) {
	dataset_t *Dataset = (dataset_t *)Args[0];
	return ml_integer(dataset_get_column_count(Dataset));
}

static ml_value_t *ml_dataset_column_open(void *Data, int Count, ml_value_t **Args) {
	dataset_t *Dataset = (dataset_t *)Args[0];
	return (ml_value_t *)dataset_column_open(Dataset, ml_string_value(Args[1]));
}

static ml_value_t *ml_dataset_column_create(void *Data, int Count, ml_value_t **Args) {
	dataset_t *Dataset = (dataset_t *)Args[0];
	const char *Name = ml_string_value(Args[1]);
	column_type_t Type = ml_integer_value(Args[2]);
	return (ml_value_t *)dataset_column_create(Dataset, Name, Type);
}

static ml_value_t *ml_column_to_string(void *Data, int Count, ml_value_t **Args) {
	column_t *Column = (column_t *)Args[0];
	return ml_string(Column->Name, -1);
}

typedef struct column_ref_t {
	const ml_type_t *Type;
	column_t *Column;
	size_t Index;
} column_ref_t;

static ml_value_t *column_ref_deref(column_ref_t *Ref) {
	column_t *Column = Ref->Column;
	switch (Column->DataType) {
	case COLUMN_STRING: {
		size_t Length = column_string_get_length(Column, Ref->Index);
		char *Buffer = GC_malloc_atomic(Length);
		column_string_get_value(Column, Ref->Index, Buffer);
		Buffer[Length] = 0;
		return ml_string(Buffer, Length);
	}
	case COLUMN_REAL: {
		return ml_real(column_real_get(Column, Ref->Index));
	}
	}
	return MLNil;
}

static ml_value_t *column_ref_assign(column_ref_t *Ref, ml_value_t *Value) {
	column_t *Column = Ref->Column;
	switch (Column->DataType) {
	case COLUMN_STRING: {
		if (Value->Type != MLStringT) return ml_error("TypeError", "Expected string");
		column_string_set(Column, Ref->Index, ml_string_value(Value), ml_string_length(Value));
		break;
	}
	case COLUMN_REAL: {
		if (Value->Type == MLIntegerT) {
			column_real_set(Column, Ref->Index, ml_integer_value(Value));
		} else if (Value->Type == MLRealT) {
			column_real_set(Column, Ref->Index, ml_real_value(Value));
		} else {
			return ml_error("TypeError", "Expected number");
		}
		break;
	}
	}
	return Value;
}


static ml_type_t ColumnRefT[1] = {{
	MLTypeT,
	MLAnyT, "column-ref",
	ml_default_hash,
	ml_default_call,
	(void *)column_ref_deref,
	(void *)column_ref_assign,
	ml_default_iterate,
	ml_default_current,
	ml_default_next,
	ml_default_key
}};

static ml_value_t *ml_column_index(void *Data, int Count, ml_value_t **Args) {
	column_ref_t *ColumnRef = new(column_ref_t);
	ColumnRef->Type = ColumnRefT;
	ColumnRef->Column = (column_t *)Args[0];
	ColumnRef->Index = ml_integer_value(Args[1]);
	return (ml_value_t *)ColumnRef;
}

void dataset_init(stringmap_t *Globals) {
	DatasetT = ml_type(MLAnyT, "dataset");
	ColumnT = ml_type(MLAnyT, "column");
	stringmap_insert(Globals, "dataset_open", ml_function(NULL, ml_dataset_open));
	stringmap_insert(Globals, "dataset_create", ml_function(NULL, ml_dataset_create));
	stringmap_insert(Globals, "COLUMN_REAL", ml_integer(COLUMN_REAL));
	stringmap_insert(Globals, "COLUMN_STRING", ml_integer(COLUMN_STRING));
	ml_method_by_name("column_count", NULL, ml_dataset_column_count, DatasetT, NULL);
	ml_method_by_name("column_open", NULL, ml_dataset_column_open, DatasetT, MLStringT, NULL);
	ml_method_by_name("column_create", NULL, ml_dataset_column_create, DatasetT, MLStringT, MLIntegerT, NULL);
	ml_method_by_name("string", NULL, ml_column_to_string, ColumnT, NULL);
	ml_method_by_name("[]", NULL, ml_column_index, ColumnT, NULL);
}
