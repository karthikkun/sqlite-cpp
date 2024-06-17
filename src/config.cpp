// Config.cpp
#include "config.h"

Config* Config::instance = nullptr;

Config::Config() : textEncoding(SQLiteEncoding::SQLITE_UTF8) {}  // Initialize to UTF-8 by default.

Config* Config::getInstance() {
    if (instance == nullptr) {
        instance = new Config();
    }
    return instance;
}

SQLiteEncoding Config::getTextEncoding() const {
    return textEncoding;
}

void Config::setTextEncoding(SQLiteEncoding encoding) {
    textEncoding = encoding;
}
