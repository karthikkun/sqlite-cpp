#include "utility.h"
#include <iostream>
#include <typeinfo>
#include "config.h"

DataType resolveSerialType(varint serialType) {
    switch (serialType) {
        case 0: return DataType::TypeNull;
        case 1: return DataType::TypeInt8;
        case 2: return DataType::TypeInt16;
        case 4: return DataType::TypeInt32;
        case 6: return DataType::TypeInt64;
        case 7: return DataType::TypeFloat64;
        case 8:
        case 9: return DataType::Unsupported;  // Special handling for fixed integers:: integer 0 | integer 1
        default:
            if (serialType >= 12 && serialType % 2 == 0)
                return DataType::TypeBlob;
            else if (serialType >= 13 && serialType % 2 == 1)
                return DataType::TypeText;
            return DataType::Unsupported;
    }
}

Data processBySerialType(varint serialType, std::istream& stream) {
    DataType dataType = resolveSerialType(serialType);
    switch (dataType) {
        case DataType::TypeInt8:
            return processData<int8_t>(stream, dataType);
            break;
        case DataType::TypeInt16:
            return processData<int16_t>(stream, dataType);
            break;
        case DataType::TypeInt32:
            return processData<int32_t>(stream, dataType);
            break;
        case DataType::TypeInt64:
            return processData<int64_t>(stream, dataType);
            break;
        case DataType::TypeFloat64:
            return processData<double>(stream, dataType);
            break;
        case DataType::TypeNull: {
            Data data;
            data.type = dataType;
            // std::cout << "Type is NULL, no data to process." << std::endl;
            return data;
            break;
        }
        case DataType::TypeText: {
            int nBytes = (serialType - 13) / 2;
            char* text = new char[nBytes + 1];  // +1 for null terminator
            SQLiteEncoding textEncoding = Config::getInstance()->getTextEncoding();
            readText(text, nBytes, textEncoding, stream);
            text[nBytes] = '\0';  // Ensure null termination

            Data data;
            data.type = DataType::TypeText;
            data.value.text = text;  // Store the dynamically allocated text
            return data;
            break;
        }
        case DataType::TypeBlob:
        case DataType::Unsupported:
        default:
            throw std::invalid_argument("Unsupported data type for processing.");
    }
}

template<typename T>
Data processData(std::istream& stream, DataType type) {
    T value;
    if (stream.read(reinterpret_cast<char*>(&value), sizeof(T))) {
        Data data;
        data.type = type;
        switch (type) {
            case DataType::TypeInt8: data.value = DataUnion(static_cast<int8_t>(value)); break;
            case DataType::TypeInt16: data.value = DataUnion(static_cast<int16_t>(value)); break;
            case DataType::TypeInt32: data.value = DataUnion(static_cast<int32_t>(value)); break;
            case DataType::TypeInt64: data.value = DataUnion(static_cast<int64_t>(value)); break;
            case DataType::TypeFloat64: data.value = DataUnion(static_cast<double>(value)); break;
            default:
                throw std::runtime_error("Invalid data type for union initialization");
        }
        return data;
    } else {
        throw std::runtime_error("Failed to read data from stream.");
    }
}

void initDataTable(DataTable *table) {
    table->rows = NULL;
    table->num_rows = 0;
}

void addRow(DataTable *table, Data *rowData, int numColumns) {
    // Resize the table to add a new row
    table->rows = (DataRow*) realloc(table->rows, (table->num_rows + 1) * sizeof(DataRow));
    table->rows[table->num_rows].columns = (Data*) malloc(numColumns * sizeof(Data));
    table->rows[table->num_rows].num_columns = numColumns;
    for (int i = 0; i < numColumns; i++) {
        table->rows[table->num_rows].columns[i] = rowData[i];
    }
    table->num_rows++;
}

void freeDataTable(DataTable *table) {
    if (table == NULL) return;

    // Iterate through each row in the table
    for (int i = 0; i < table->num_rows; i++) {
        // Check if the row has columns
        if (table->rows[i].columns != NULL) {
            // Iterate through each column in the row
            for (int j = 0; j < table->rows[i].num_columns; j++) {
                // Free the text in each column if it exists
                if (table->rows[i].columns[j].type == DataType::TypeText)
                    free(table->rows[i].columns[j].value.text);
            }
            // Free the array of columns
            free(table->rows[i].columns);
        }
    }
    // Finally, free the rows array
    free(table->rows);
    // Set the pointer to NULL to avoid dangling pointer issues
    table->rows = NULL;
    table->num_rows = 0;
}

// Function to swap the byte order of a uint32_t from big-endian to little-endian
uint32_t swapEndian(uint32_t val) {
    return ((val & 0x000000FF) << 24) | 
           ((val & 0x0000FF00) << 8) |
           ((val & 0x00FF0000) >> 8) |
           ((val & 0xFF000000) >> 24);
}

//  4-byte big-endian integer at offset 56
SQLiteEncoding getTextEncoding(std::istream& stream) {
    uint32_t encodingType;
    stream.seekg(56);
    stream.read(reinterpret_cast<char*>(&encodingType), sizeof(encodingType));
    encodingType = swapEndian(encodingType);
    return static_cast<SQLiteEncoding>(encodingType);
}

void readText(char* text, size_t nBytes, SQLiteEncoding textEncoding, std::istream& stream) {
    switch(textEncoding) {
        // UTF-8 is a variable-width encoding where each character can range from 1 to 4 bytes
        case SQLiteEncoding::SQLITE_UTF8: {
            stream.read(text, nBytes);
            //TODO: handle failure | character boundaries
            break;
        }
        case SQLiteEncoding::SQLITE_UTF16BE: {
            break;
        }
        case SQLiteEncoding::SQLITE_UTF16LE: {
            break;
        }
        default:
            throw std::runtime_error("Invalid Text Encoding!! Unable to proceed");
    }
}