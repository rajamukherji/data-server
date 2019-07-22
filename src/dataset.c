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
	stringmap_t Watchers[1];
	json_int_t Generation;
	size_t MapSize;
	column_type_t DataType;
	int Fd;
};

struct dataset_t {
	const ml_type_t *Type;
	const char *Path, *Name, *InfoFile;
	json_t *Info;
	stringmap_t Columns[1];
	stringmap_t Watchers[1];
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
}

int column_string_extend_hint(column_t *Column, size_t Index, int Length) {
	if (Index >= Column->Dataset->Length) return 0;
	string_node_t *Nodes = (string_node_t *)(Column->Strings->Entries + Column->Dataset->Length);
	int OldLength = Column->Strings->Entries[Index].Length;
	int NumBlocksOld = 1 + (OldLength - 5) / 12;
	if (NumBlocksOld < 1) NumBlocksOld = 1;
	int NumBlocksNew = 1 + (Length - 5) / 12;
	if (NumBlocksNew < 1) NumBlocksNew = 1;
	return NumBlocksNew - NumBlocksOld;
}

void column_string_extend(column_t *Column, int Required) {
	int FreeCount = Column->Strings->Header.FreeCount;
	if (Required > FreeCount) {
		int Shortfall = Required - FreeCount;
		size_t MapSize = Column->MapSize + Shortfall * sizeof(string_node_t);
		msync(Column->Map, Column->MapSize, MS_SYNC);
		ftruncate(Column->Fd, MapSize);
		Column->Map = mremap(Column->Map, Column->MapSize, MapSize, MREMAP_MAYMOVE);
		string_node_t *Nodes = (string_node_t *)(Column->Strings->Entries + Column->Dataset->Length);
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
		Column->Strings->Header.FreeCount = Required;
	}
}

double column_real_get(column_t *Column, size_t Index) {
	return Column->Reals[Index];
}

void column_real_set(column_t *Column, size_t Index, double Value) {
	Column->Reals[Index] = Value;
	msync(Column->Map, Column->MapSize, MS_ASYNC);
}

void column_watcher_add(column_t *Column, const char *Key, void *Value) {
	stringmap_insert(Column->Watchers, Key, Value);
}

void column_watcher_remove(column_t *Column, const char *Key) {
	stringmap_remove(Column->Watchers, Key);
}

void column_watcher_foreach(column_t *Column, void *Data, int (*Callback)(const char *, void *, void *)) {
	stringmap_foreach(Column->Watchers, Data, Callback);
}

json_int_t column_generation_bump(column_t *Column) {
	return ++Column->Generation;
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
	column_t *Images = dataset_column_create(Dataset, "image", COLUMN_STRING);
	json_object_set(Dataset->Info, "image", json_string(Images->Id));
	json_dump_file(Dataset->Info, Dataset->InfoFile, 0);
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
		const char *DataType;
		json_unpack(ColumnJson, "{ssss}", "name", &Column->Name, "type", &DataType);
		if (!strcmp(DataType, "string")) {
			Column->DataType = COLUMN_STRING;
		} else if (!strcmp(DataType, "real")) {
			Column->DataType = COLUMN_REAL;
		}
		stringmap_insert(Dataset->Columns, Id, Column);
	}
	const char *ImageId = json_string_value(json_object_get(Dataset->Info, "image"));
	column_t *Column = new(column_t);
	Column->Type = ColumnT;
	Column->Dataset = Dataset;
	Column->Id = ImageId;
	Column->DataType = COLUMN_STRING;
	stringmap_insert(Dataset->Columns, ImageId, Column);
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

json_t *dataset_get_column_info(dataset_t *Dataset, const char *Id) {
	return json_object_get(json_object_get(Dataset->Info, "columns"), Id);
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
	const char *DataType;
	switch (Type) {
	case COLUMN_STRING: {
		DataType = "string";
		Column->MapSize = sizeof(string_header_t) + Dataset->Length * sizeof(string_entry_t) + Dataset->Length * sizeof(string_node_t);
		ftruncate(Column->Fd, Column->MapSize);
		Column->Map = mmap(NULL, Column->MapSize, PROT_READ | PROT_WRITE, MAP_SHARED, Column->Fd, 0);
		Column->Strings->Header.FreeCount = 0;
		Column->Strings->Header.FreeStart = 0;
		for (int I = 0; I < Dataset->Length; ++I) Column->Strings->Entries[I].Link = I;
		break;
	}
	case COLUMN_REAL: {
		DataType = "real";
		Column->MapSize = Dataset->Length * sizeof(double);
		ftruncate(Column->Fd, Column->MapSize);
		Column->Map = mmap(NULL, Column->MapSize, PROT_READ | PROT_WRITE, MAP_SHARED, Column->Fd, 0);
		break;
	}
	}
	stringmap_insert(Dataset->Columns, Column->Id, Column);
	if (json_object_get(Dataset->Info, "image")) {
		json_t *ColumnsJson = json_object_get(Dataset->Info, "columns");
		json_object_set(ColumnsJson, Column->Id, json_pack("{ssss}", "name", Name, "type", DataType));
		json_dump_file(Dataset->Info, Dataset->InfoFile, 0);
	}
	msync(Column->Map, Column->MapSize, MS_ASYNC);
	return Column;
}

column_t *dataset_column_open(dataset_t *Dataset, const char *Id) {
	column_t *Column = stringmap_search(Dataset->Columns, Id);
	if (!Column) return NULL;
	if (!Column->Map) column_open(Column);
	return Column;
}

void dataset_watcher_add(dataset_t *Dataset, const char *Key, void *Value) {
	stringmap_insert(Dataset->Watchers, Key, Value);
}

static int column_watcher_remove_fn(const char *Id, column_t *Column , const char *Key) {
	stringmap_remove(Column->Watchers, Key);
	return 0;
}

void dataset_watcher_remove(dataset_t *Dataset, const char *Key) {
	stringmap_remove(Dataset->Watchers, Key);
	stringmap_foreach(Dataset->Columns, (void *)Key, (void *)column_watcher_remove_fn);
}

void dataset_watcher_foreach(dataset_t *Dataset, void *Data, int (*Callback)(const char *, void *, void *)) {
	stringmap_foreach(Dataset->Watchers, Data, Callback);
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

static ml_value_t *ml_column_extend_hint(void *Data, int Count, ml_value_t **Args) {
	column_t *Column = (column_t *)Args[0];
	size_t Index = ml_integer_value(Args[1]);
	int Length = ml_integer_value(Args[2]);
	return ml_integer(column_string_extend_hint(Column, Index, Length));
}

static ml_value_t *ml_column_extend(void *Data, int Count, ml_value_t **Args) {
	column_t *Column = (column_t *)Args[0];
	int Required = ml_integer_value(Args[1]);
	column_string_extend(Column, Required);
	return Args[0];
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
	ml_method_by_name("extend_hint", NULL, ml_column_extend_hint, ColumnT, MLIntegerT, MLIntegerT, NULL);
	ml_method_by_name("extend", NULL, ml_column_extend, ColumnT, MLIntegerT, NULL);
}
