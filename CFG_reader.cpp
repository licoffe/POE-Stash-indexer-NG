/*
 *  CFG_reader.cpp
 *  POE-Indexer-NG
 *
 *  Created by Licoffe on December 14th 2016.
 *  Distributed under MIT license, See file LICENSE for detail
 * -----------------------------------------------------------------------------
 *  Description
 *  
 *  Contains minimal code to parse configuration files
 */

#include "CFG_reader.h"

/**
 * Parse CFG file and store keys and values in the fields map
 *
 * @param Path to CFG file
 * @return Nothing 
 */
CFG_reader::CFG_reader( const std::string path ) {
    std::ifstream infile( path );
    std::string line;
    std::smatch match;
    while ( std::getline( infile, line )) {
        if ( std::regex_match( line, CFG_reader::COMMENT_RE )) {
        } else if ( std::regex_search( line, match, CFG_reader::DEFINITION_RE )) {
            this->fields[match[1]] = match[2];
        } else {
            std::cout << "Invalid line: " << line << std::endl;
        }
    }
}

/**
 * Get parsed value associated to field
 *
 * @param Field to fetch
 * @return Value associated to input field 
 */
std::string CFG_reader::get( const std::string field ) {
    return this->fields.find( field )->second;
}