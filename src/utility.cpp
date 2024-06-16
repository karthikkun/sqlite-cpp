#include "utility.h"
#include <iostream>
#include <typeinfo>

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
    for (int i = 0; i < table->num_rows; i++) {
        free(table->rows[i].columns);
    }
    free(table->rows);
}