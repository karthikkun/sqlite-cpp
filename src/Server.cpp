#include <cstring>
#include <iostream>
#include <fstream>

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

    if (command == ".dbinfo") {
        std::ifstream db_file(db_file_path, std::ios::binary);
        if (!db_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }
        
        // Skip the first 16 bytes of the header
        db_file.seekg(16);
        
        char buffer[2];
        db_file.read(buffer, 2);
        
        // little-endian format
        unsigned short page_size = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
        
        std::cout << "database page size: " << page_size << std::endl;

        db_file.close();
    }

    return 0;
}
