// Config.h
#ifndef CONFIG_H
#define CONFIG_H
#include "utility.h"

class Config {
private:
    static Config* instance;
    SQLiteEncoding textEncoding;

    Config();  // Private constructor.
    Config(const Config&) = delete;  // Prevent copying.
    Config& operator=(const Config&) = delete;  // Prevent assignment.

public:
    static Config* getInstance();
    SQLiteEncoding getTextEncoding() const;
    void setTextEncoding(SQLiteEncoding encoding);
};

#endif // CONFIG_H
