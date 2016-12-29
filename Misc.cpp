/*
 *  Misc.cpp
 *  POE-Indexer-NG
 *
 *  Created by Licoffe on December 14th 2016.
 *  Distributed under MIT license, See file LICENSE for detail
 * -----------------------------------------------------------------------------
 *  Description
 *  
 *  Misc useful functions
 */

#include "Misc.h"

/**
 * Get UNIX timestamp in milliseconds
 *
 * @param Nothing
 * @return UNIX timestamp in milliseconds 
 */
std::chrono::milliseconds get_current_timestamp() {
    return std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch()
    );
}

/**
 * Print date and time
 *
 * @param Nothing
 * @return String representing date and time 
 */
#if defined __gnu_linux__ && __GNUC__ < 5
std::string date() {
    time_t rawtime;
    struct tm * timeinfo;
    char time2 [24];

    time (&rawtime);
    timeinfo = localtime (&rawtime);
    strftime( time2, sizeof( time2 ), "%Y-%m-%d %X", timeinfo );

    std::stringstream ss;
    ss << time2;
    return ss.str();
}
#else
std::string date() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
    return ss.str();
}
#endif

/**
 * Replace all iteration of a string in input string 
 *
 * @param String in which to search for pattern, pattern, text to replace with
 * @return Modified original string 
 */
// http://stackoverflow.com/questions/4643512/replace-substring-with-another-substring-c
std::string replace_string( std::string subject, const std::string& search,
                            const std::string& replace) {
    size_t pos = 0;
    while (( pos = subject.find( search, pos )) != std::string::npos ) {
         subject.replace( pos, search.length(), replace );
         pos += replace.length();
    }
    return subject;
}

/**
 * Return current working directory
 *
 * @param Nothing
 * @return Current working directory 
 */
std::string get_current_dir() {
    char * cwd;
    cwd = (char*) malloc( FILENAME_MAX * sizeof( char ));
    getcwd( cwd, FILENAME_MAX );
    return std::string( cwd );
}