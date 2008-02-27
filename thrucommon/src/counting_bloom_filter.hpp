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


class counting_bloom_filter
{
public:

   typedef unsigned int bloom_type;

   counting_bloom_filter(const std::size_t& element_count,
                const double& false_positive_probability,
                const std::size_t& random_seed)
   : hash_table_(0),
     element_count_(element_count),
     random_seed_(random_seed),
     false_positive_probability_(false_positive_probability)
   {
      find_optimal_parameters();
      hash_table_ = new unsigned char[table_size_];
      generate_unique_salt();
      for (std::size_t i = 0; i < (table_size_); i++)
      {
         hash_table_[i] = static_cast<unsigned char>(0x0);
      }
   }

   counting_bloom_filter(const counting_bloom_filter& filter)
   {
      this->operator =(filter);
   }

   counting_bloom_filter& operator = (const counting_bloom_filter& filter)
   {
      salt_count_          = filter.salt_count_;
      table_size_          = filter.table_size_;
      element_count_       = filter.element_count_;
      random_seed_         = filter.random_seed_;
      false_positive_probability_ = filter.false_positive_probability_;
      delete[] hash_table_;
      hash_table_ = new unsigned char[table_size_];
      std::copy(filter.hash_table_,filter.hash_table_ + (table_size_),hash_table_);
      salt_.clear();
      std::copy(filter.salt_.begin(),filter.salt_.end(),std::back_inserter(salt_));
      return *this;
   }

  ~counting_bloom_filter()
   {
      delete[] hash_table_;
   }

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

   std::size_t size() { return table_size_; }

   counting_bloom_filter& operator&=(const counting_bloom_filter& filter)
   {
      /* intersection */
      if (
          (salt_count_  == filter.salt_count_) &&
          (table_size_  == filter.table_size_) &&
          (random_seed_ == filter.random_seed_)
         )
      {
         for (std::size_t i = 0; i < (table_size_); ++i)
         {
            hash_table_[i] &= filter.hash_table_[i];
         }
      }
      return *this;
   }

   counting_bloom_filter& operator|=(const counting_bloom_filter& filter)
   {
      /* union */
      if (
          (salt_count_  == filter.salt_count_) &&
          (table_size_  == filter.table_size_) &&
          (random_seed_ == filter.random_seed_)
         )
      {
         for (std::size_t i = 0; i < (table_size_); ++i)
         {
            hash_table_[i] |= filter.hash_table_[i];
         }
      }
      return *this;
   }

   counting_bloom_filter& operator^=(const counting_bloom_filter& filter)
   {
      /* difference */
      if (
          (salt_count_  == filter.salt_count_) &&
          (table_size_  == filter.table_size_) &&
          (random_seed_ == filter.random_seed_)
         )
      {
         for (std::size_t i = 0; i < (table_size_); ++i)
         {
            hash_table_[i] ^= filter.hash_table_[i];
         }
      }
      return *this;
   }

private:

   void generate_unique_salt()
   {
      const unsigned int predef_salt_count = 32;
      const bloom_type predef_salt[predef_salt_count] =
                       {
                         0xAAAAAAAA, 0x55555555, 0x33333333, 0xCCCCCCCC,
                         0x66666666, 0x99999999, 0xB5B5B5B5, 0x4B4B4B4B,
                         0xAA55AA55, 0x55335533, 0x33CC33CC, 0xCC66CC66,
                         0x66996699, 0x99B599B5, 0xB54BB54B, 0x4BAA4BAA,
                         0xAA33AA33, 0x55CC55CC, 0x33663366, 0xCC99CC99,
                         0x66B566B5, 0x994B994B, 0xB5AAB5AA, 0xAAAAAA33,
                         0x555555CC, 0x33333366, 0xCCCCCC99, 0x666666B5,
                         0x9999994B, 0xB5B5B5AA, 0xFFFFFFFF, 0xFFFF0000
                       };
      if (salt_count_ <= predef_salt_count)
      {
         std::copy(predef_salt,predef_salt + salt_count_,std::back_inserter(salt_));
      }
      else
      {
         std::copy(predef_salt,predef_salt + predef_salt_count,std::back_inserter(salt_));
         srand(static_cast<unsigned int>(0xAAAAAAAA ^ random_seed_));
         while(salt_.size() < salt_count_)
         {
            bloom_type current_salt = static_cast<bloom_type>(rand()) ^ static_cast<bloom_type>(rand());
            if (current_salt == 0) continue;
            bool duplicate_found = false;
            for(std::vector<bloom_type>::iterator it = salt_.begin(); it != salt_.end(); ++it)
            {
               if (current_salt == (*it))
               {
                  duplicate_found = true;
                  break;
               }
            }
            if (!duplicate_found)
            {
               salt_.push_back(current_salt);
            }
         }
      }
   }

   void find_optimal_parameters()
   {
      double min_m  = std::numeric_limits<double>::infinity();
      double min_k  = 0.0;
      double curr_m = 0.0;
      for(double k = 0; k < 1000.0; k++)
      {
         if ((curr_m = ((- k * element_count_) / std::log(1 - std::pow(false_positive_probability_,1 / k)))) < min_m)
         {
            min_m = curr_m;
            min_k = k;
         }
      }
      salt_count_  = static_cast<std::size_t>(min_k);
      table_size_  = static_cast<std::size_t>(min_m);
   }

   bloom_type hash_ap(const std::string& str,bloom_type hash) const
   {
      for(std::size_t i = 0; i < str.length(); i++)
      {
         hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ str[i] ^ (hash >> 3)) :
                                  (~((hash << 11) ^ str[i] ^ (hash >> 5)));
      }
      return hash;
   }

   std::vector<bloom_type> salt_;
   unsigned char*          hash_table_;
   std::size_t             salt_count_;
   std::size_t             table_size_;
   std::size_t             element_count_;
   std::size_t             random_seed_;
   double                  false_positive_probability_;
};


counting_bloom_filter operator & (const counting_bloom_filter& a, const counting_bloom_filter& b)
{
   counting_bloom_filter result = a;
   result &= b;
   return result;
}

counting_bloom_filter operator | (const counting_bloom_filter& a, const counting_bloom_filter& b)
{
   counting_bloom_filter result = a;
   result |= b;
   return result;
}

counting_bloom_filter operator ^ (const counting_bloom_filter& a, const counting_bloom_filter& b)
{
   counting_bloom_filter result = a;
   result ^= b;
   return result;
}


#endif



/*
   Note:
   If it can be guaranteed that bits_per_char will be of the form 2^n then
   the following optimization can be used:

   hash_table[bit_index >> n] |= bit_mask[bit_index & (bits_per_char - 1)];

*/
