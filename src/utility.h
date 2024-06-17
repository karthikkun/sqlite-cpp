#ifndef UTILITY_H
#define UTILITY_H

#include <stdexcept>
#include <string>
#include <cstdint>
#define varint int64_t

typedef enum {
    SQLITE_UTF8 = 1,     // UTF-8 text encoding
    SQLITE_UTF16LE = 2,  // UTF-16 little-endian text encoding
    SQLITE_UTF16BE = 3   // UTF-16 big-endian text encoding
} SQLiteEncoding;

// Enum representing data types handled by the processor
enum class DataType {
    TypeNull,
    TypeInt8,
    TypeInt16,
    TypeInt32,
    TypeInt64,
    TypeFloat64,
    TypeBlob,
    TypeText,
    Unsupported
};

union DataUnion {
    int8_t int8;
    int16_t int16;
    int32_t int32;
    int64_t int64;
    double float64;
    char* text;

    DataUnion() {}  // Default constructor

    // Constructors for each type
    DataUnion(int8_t val) : int8(val) {}
    DataUnion(int16_t val) : int16(val) {}
    DataUnion(int32_t val) : int32(val) {}
    DataUnion(int64_t val) : int64(val) {}
    DataUnion(double val) : float64(val) {}
};

struct Data {
    DataType type;
    DataUnion value;
};

typedef struct {
    Data *columns;
    int num_columns;
} DataRow;

typedef struct {
    DataRow *rows;
    int num_rows;
} DataTable;

// Function to resolve the data type from a serial type code
DataType resolveSerialType(varint serialType);

// Template function to process data based on its type
template<typename T>
Data processData(std::istream& stream, DataType type);

// Function to process data by its serial type code
Data processBySerialType(varint serialType, std::istream& stream);


void initDataTable(DataTable *table);
void addRow(DataTable *table, Data *rowData, int numColumns);
void freeDataTable(DataTable *table);

SQLiteEncoding getTextEncoding(std::istream& stream);

void readText(char* text, size_t nBytes, SQLiteEncoding textEncoding, std::istream& stream);

#endif // UTILITY_H
