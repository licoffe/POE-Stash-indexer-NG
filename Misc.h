/*
 *  Misc.h
 *  POE-Indexer-NG
 *
 *  Created by Licoffe on December 14th 2016.
 *  Distributed under MIT license, See file LICENSE for detail
 * -----------------------------------------------------------------------------
 *  Description
 *  
 *  Misc useful functions
 */

#ifndef _MISC_H_
#define _MISC_H_

#include <chrono>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <string>
#include <unistd.h>

std::chrono::milliseconds get_current_timestamp();

std::string date();

std::string replace_string( std::string, const std::string&, const std::string& );

std::string get_current_dir();

#endif /* _MISC_H_ */