#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>
#include "utility.h"
// #include <cstdint>

#define DATABASE_HEADER 100
#define TABLE_LEAF_PAGE_HEADER 8
#define CELL_POINTER 2
#define varint int64_t

static unsigned short PAGE_SIZE;
static unsigned short RESERVED_SIZE;

varint readVarint(std::istream& stream) {
    varint value = 0;
    char buffer;
    int pos = 0;
    while (pos++ < 9) {
        stream.read(&buffer, 1);
        if (stream.gcount() != 1) {
            throw std::runtime_error("Failed to read 1 byte from stream.");
        }
        unsigned short byte = static_cast<unsigned char>(buffer);
        if(pos == 9) {
            value <<= 8;    
            value |= byte;
            continue;
        }
        value <<= 7;
        value |= byte & 0x7F;
        if ((byte & 0x80) == 0) break; // MSB is not set. Last Byte
    }
    return value;
}

// read big endian 4 byte int from current read positon of an input stream
uint32_t read4ByteInt(std::istream& stream) {
    char buffer[4];
    stream.read(buffer, 4);
    if (stream.gcount() != 4) {
        throw std::runtime_error("Failed to read 4 bytes from stream.");
    }

    return (static_cast<unsigned char>(buffer[0]) << 24) |
           (static_cast<unsigned char>(buffer[1]) << 16) |
           (static_cast<unsigned char>(buffer[2]) << 8) |
            static_cast<unsigned char>(buffer[3]);
}

int main(int argc, char* argv[]) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string db_file_path = argv[1];
    std::string command = argv[2];

    std::ifstream db_file(db_file_path, std::ios::binary);
    if (!db_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return 1;
    }
    
    char buffer[2];
    
    // database page size in bytes | 2 bytes
    db_file.seekg(16);
    db_file.read(buffer, 2);
    PAGE_SIZE = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));

    // Bytes of unused "reserved" space at the end of each page | 1 byte
    db_file.seekg(20);
    db_file.read(buffer, 1);
    RESERVED_SIZE = static_cast<unsigned char>(buffer[0]);

    if (command == ".dbinfo") {
        std::cout << "database page size: " << PAGE_SIZE << std::endl;

        //  table b-tree leaf page
        // Skip database header | offset 3 to reach cell count
        db_file.seekg(DATABASE_HEADER + 3);
        db_file.read(buffer, 2);
        unsigned short table_count = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
        std::cout << "number of tables: " << table_count << std::endl;
    }
    else if(command == ".tables") {
        DataTable table;
        initDataTable(&table);

        // Skip database header | offset 3 to reach cell count
        char buffer[2];
        db_file.seekg(DATABASE_HEADER + 3);
        db_file.read(buffer, 2);
        unsigned short cell_count = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
        
        int cnt = 0;
        while (cnt++ < cell_count) {
            db_file.seekg(DATABASE_HEADER + TABLE_LEAF_PAGE_HEADER + cnt*CELL_POINTER);
            // read cell content offset into buffer
            db_file.read(buffer, 2);
            unsigned short cell_content_offset = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
            db_file.seekg(cell_content_offset);

            // Number of bytes of payload varint ~ `P`
            varint nbytes_payload = readVarint(db_file);
            std::cout << nbytes_payload << std::endl;
            // Rowid varint
            varint row_id = readVarint(db_file);
            std::cout << row_id << std::endl;            

            /**
             * U = PAGE_SIZE - RESERVED_SIZE
             * U - 35 essentially gives you the maximum amount of data (payload) that can be stored \
             * in the body of the B-tree page without the data spilling 
             */ 
            unsigned short U = PAGE_SIZE - RESERVED_SIZE;
            // Payload byte[]
            char* payload = new char[nbytes_payload];  // Allocate payload buffer
            // read inital payload
            // TODO: Warning! din't handle when payload overflowed
            if (nbytes_payload > U - 35) throw std::runtime_error("Payload size exceeds the maximum allowed limit.");
            
            db_file.read(payload, nbytes_payload);

            uint32_t overflow_page_no;
            if (nbytes_payload > U - 35) {
                // Page number of first overflow page 4-byte integer present
                uint32_t overflow_page_no = read4ByteInt(db_file);
                // TODO: read remaining payload from overflow pages
                throw std::runtime_error("Payload size exceeds the maximum allowed limit.");
            }

            std::string str_payload(payload, nbytes_payload);
            std::istringstream payload_stream(str_payload);

            // Record
            // record header size
            varint nbytes_record_header = readVarint(payload_stream);
            /**
             * CREATE TABLE sqlite_schema(
                    type text,
                    name text,
                    tbl_name text,
                    rootpage integer,
                    sql text
                );
             */
            // Serial Type Codes
            varint sType = readVarint(payload_stream);
            varint sName = readVarint(payload_stream);
            varint sTblName = readVarint(payload_stream);
            varint sRootpage = readVarint(payload_stream);
            varint sSql = readVarint(payload_stream);

            // Record body
            Data type = processBySerialType(sType, payload_stream);
            Data name = processBySerialType(sName, payload_stream);
            Data tblName = processBySerialType(sTblName, payload_stream);
            Data rootPage = processBySerialType(sRootpage, payload_stream);
            Data sql = processBySerialType(sSql, payload_stream);
            
            Data* row = (Data*)malloc(5*sizeof(Data));
            row[0] = type; row[1] = name; row[2] = tblName;
            row[3] = rootPage; row[4] = sql;
            addRow(&table, row, 5);
        }
        freeDataTable(&table);
    }

    db_file.close();
    return 0;
}
