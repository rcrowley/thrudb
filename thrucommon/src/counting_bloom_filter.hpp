/*
 **************************************************************************
 *                                                                        *
 *                           Open Bloom Filter                            *
 *                                                                        *
 * Author: Arash Partow - 2000                                            *
 * URL: http://www.partow.net                                             *
 * URL: http://www.partow.net/programming/hashfunctions/index.html        *
 *                                                                        *
 * Copyright notice:                                                      *
 * Free use of the Bloom Filter Library is permitted under the guidelines *
 * and in accordance with the most current version of the Common Public   *
 * License.                                                               *
 * http://www.opensource.org/licenses/cpl.php                             *
 *                                                                        *
 **************************************************************************
*/


#ifndef COUNTING_BLOOM_FILTER_HPP
#define COUNTING_BLOOM_FILTER_HPP

#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <limits>

#include "bloom_filter.hpp"

class counting_bloom_filter : public bloom_filter
{
public:

   counting_bloom_filter(const std::size_t& element_count,
                const double& false_positive_probability,
                const std::size_t& random_seed)
       : bloom_filter(element_count,false_positive_probability,random_seed,0x01)
   { }

   void insert(const std::string key)
   {
      for(std::vector<bloom_type>::iterator it = salt_.begin(); it != salt_.end(); ++it)
      {
         std::size_t bit_index = hash_ap(key,(*it)) % table_size_;

         if( hash_table_[bit_index] < 255)
             hash_table_[bit_index]++;
      }
   }

   void remove(const std::string key)
   {
      for(std::vector<bloom_type>::iterator it = salt_.begin(); it != salt_.end(); ++it)
      {
         std::size_t bit_index = hash_ap(key,(*it)) % table_size_;

         if( hash_table_[bit_index] > 0)
             hash_table_[bit_index]--;
      }
   }

   bool contains(const std::string key) const
   {
      for(std::vector<bloom_type>::const_iterator it = salt_.begin(); it != salt_.end(); ++it)
      {
         std::size_t bit_index = hash_ap(key,(*it)) % table_size_;

         if (hash_table_[bit_index] == 0)
         {
            return false;
         }
      }
      return true;
   }

};

#endif


