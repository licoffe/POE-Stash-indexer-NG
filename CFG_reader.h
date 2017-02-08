/*
 *  CFG_reader.h
 *  POE-Indexer-NG
 *
 *  Created by Licoffe on December 14th 2016.
 *  Distributed under MIT license, See file LICENSE for detail
 * -----------------------------------------------------------------------------
 *  Description
 *  
 *  Contains minimal code to parse configuration files
 */

#ifndef _CFG_READER_H_
#define _CFG_READER_H_

#include <string>
#include <map>
#include <fstream>
#include <sstream>
#include <regex>
#include <iostream>

class CFG_reader {
    public:
                                           CFG_reader( const std::string path );
        std::string                        get( const std::string field );
        std::map<std::string, std::string> fields; // holds parsed values
    private:
        const std::regex COMMENT_RE    = std::regex( "^#.*" );
        const std::regex DEFINITION_RE = 
            std::regex( "([a-zA-Z_]+)\\s*=\\s*([a-zA-Z0-9_:/. \-]+)" );
};

#endif /* _CFG_READER_H_ */