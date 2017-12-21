#pragma once

#include <string>
#include <memory>
#include <type_traits>
#include <thread>
#include <boost/uuid/name_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
//#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

namespace timax
{
	inline void encode_fixed_32(char* dst, uint64_t value)
	{
		dst[0] = value & 0xff;
		dst[1] = (value >> 8) & 0xff;
		dst[2] = (value >> 16) & 0xff;
		dst[3] = (value >> 24) & 0xff;
	}

	inline void encode_fixed_64(char* dst, uint64_t value)
	{
		dst[0] = value & 0xff;
		dst[1] = (value >> 8) & 0xff;
		dst[2] = (value >> 16) & 0xff;
		dst[3] = (value >> 24) & 0xff;
		dst[4] = (value >> 32) & 0xff;
		dst[5] = (value >> 40) & 0xff;
		dst[6] = (value >> 48) & 0xff;
		dst[7] = (value >> 56) & 0xff;
	}

	inline uint32_t decode_fixed_32(char const* ptr)
	{
		return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
	}

	inline uint64_t decode_fixed_64(char const* ptr)
	{
		uint64_t const lo = decode_fixed_32(ptr);
		uint64_t const hi = decode_fixed_32(ptr + 4);
		return (hi << 32) | lo;
	}

	inline void put_fixed_32(std::string* dst, uint32_t value)
	{
		char buf[sizeof(value)];
		encode_fixed_32(buf, value);
		dst->append(buf, sizeof(buf));
	}

	inline void put_fixed_64(std::string* dst, uint64_t value)
	{
		char buf[sizeof(value)];
		encode_fixed_64(buf, value);
		dst->append(buf, sizeof(buf));
	}

	namespace detail
	{
		inline static uint32_t swap_endian(uint32_t value)
		{
			return ((value & 0x000000FF) << 24 |
				(value & 0x0000FF00) << 8 |
				(value & 0x00FF0000) >> 8 |
				(value & 0xFF000000) >> 24);
		}
	}

	/* queue index generator*/
	class queue_generator
	{
		static constexpr size_t static_key_size = boost::uuids::uuid::static_size() + sizeof(uint32_t);
	public:
		queue_generator()
			: seed_(boost::uuids::string_generator{}("c6c697f3-ca98-420b-bdbf-d4390ac025cf"))
		{
		}

		std::string operator() (std::string const& queue_name, uint32_t queue_index) const
		{
			// create generator
			boost::uuids::name_generator gen{ seed_ };

			// prepare buffer
			std::string key;
			key.resize(static_key_size);

			// generator key
			auto uuid = gen(queue_name.c_str());

			// generator index
			queue_index = swap_endian(queue_index);

			// copy
			std::copy(uuid.begin(), uuid.end(), key.begin());
			std::memcpy(&key[boost::uuids::uuid::static_size()], &queue_index, sizeof(uint32_t));

			return key;
		}

	private:

		static uint32_t swap_endian(uint32_t value)
		{
			return ((value & 0x000000FF) << 24 |
				(value & 0x0000FF00) << 8 |
				(value & 0x00FF0000) >> 8 |
				(value & 0xFF000000) >> 24);
		}

	private:
		boost::uuids::uuid const			seed_;
	};

	template <typename T>
	struct rocksdb_ingtegral_tratis;

	template <>
	struct rocksdb_ingtegral_tratis<uint64_t>
	{
		static void put(std::string* dst, uint64_t value)
		{
			put_fixed_64(dst, value);
		}

		static uint64_t decode(char const* ptr)
		{
			return decode_fixed_64(ptr);
		}

		static char const* name() noexcept
		{
			return "timax_uin64_operator";
		}

		static void encode(char* dst, uint64_t value) noexcept
		{
			encode_fixed_64(dst, value);
		}

		static constexpr uint64_t default_value() noexcept
		{
			return 1u;
		}
	};

	template <>
	struct rocksdb_ingtegral_tratis<uint32_t>
	{
		static void put(std::string* dst, uint32_t value)
		{
			put_fixed_32(dst, value);
		}

		static uint32_t decode(char const* ptr)
		{
			return decode_fixed_32(ptr);
		}

		static char const* name() noexcept
		{
			return "timax_uin32_operator";
		}

		static void encode(char* dst, uint32_t value) noexcept
		{
			encode_fixed_32(dst, value);
		}

		static constexpr uint32_t default_value() noexcept
		{
			return 1ull;
		}
	};

	/*rocksdb merge operator*/
	/*template <typename T>
	class integral_merge_operator : public rocksdb::AssociativeMergeOperator
	{
		static_assert(std::is_integral<T>::value, "Type T is not integral!");
	public:
		using value_type = T;
		using rit = rocksdb_ingtegral_tratis<value_type>;

	public:
		integral_merge_operator() = default;
		~integral_merge_operator() override = default;

		virtual bool Merge(const rocksdb::Slice& key,
			const rocksdb::Slice* existing_value,
			const rocksdb::Slice& value,
			std::string* new_value,
			rocksdb::Logger* logger) const override
		{
			assert(new_value);
			assert(new_value->empty());
			if (value.size() != sizeof(value_type))
				return false;

			value_type orig_value = rit::default_value();
			if (existing_value) 
			{
				orig_value = rit::decode(existing_value->data());
			}
			value_type operand = rit::decode(value.data());

			new_value->clear();
			rit::put(new_value, orig_value + operand);
			return true;
		}

		virtual const char* Name() const override
		{
			return rit::name();
		}
	};*/

	using normal_db_t = std::unique_ptr<rocksdb::DB>;
	using transaction_db_t = std::unique_ptr<rocksdb::TransactionDB>;

	template <typename T>
	class queue_counter
	{
		static_assert(std::is_integral<T>::value, "Type T is not integral");
		using value_type = T;
		using rit = rocksdb_ingtegral_tratis<value_type>;

	public:
		/*static bool fetch_add(rocksdb::Transaction* txn,
			rocksdb::ColumnFamilyHandle* handle,
			std::string const& key,
			value_type value)
		{
			char encoded[sizeof(value_type)];
			rit::encode(encoded, value);
			rocksdb::Slice slice(encoded, sizeof(value_type));
			auto s = txn->Merge(handle, key, slice);
			return s.ok();
		}*/

		template <typename DB>
		static bool get(DB const& db,
			rocksdb::ReadOptions const& option,
			rocksdb::ColumnFamilyHandle* handle,
			std::string const& key,
			value_type& value)
		{
			std::string str;
			auto s = db->Get(option, handle, key, &str);
			if (s.IsNotFound())
			{
				value = rit::default_value();
				return true;
			}

			if (s.ok())
			{
				if (str.size() != sizeof(value_type))
				{
					return false;
				}

				value = rit::decode(&str[0]);
				return true;
			}

			return false;
		}

		static bool put(rocksdb::Transaction* txn,
			rocksdb::ColumnFamilyHandle* handle,
			std::string const& key,
			value_type value)
		{
			char value_str[sizeof(uint32_t)];
			encode_fixed_32(value_str, value);
			auto s = txn->Put(handle, key, rocksdb::Slice{ value_str, sizeof(value_str) });
			return s.ok();
		}

		static bool get_for_update(rocksdb::Transaction* txn,
			rocksdb::ColumnFamilyHandle* handle,
			std::string const& key,
			value_type& value)
		{
			std::string str;
			auto s = txn->GetForUpdate(rocksdb::ReadOptions{}, handle, key, &str);
			if (s.IsNotFound())
			{
				value = rit::default_value();
				return true;
			}
			
			if (s.ok())
			{
				if (str.size() != sizeof(value_type))
				{
					return false;
				}

				value = rit::decode(&str[0]);
				return true;
			}

			return false;
		}
	};

	struct rocksdb_txn_rollback_guard
	{
		rocksdb_txn_rollback_guard(rocksdb::Transaction* txn)
			: txn_(txn)
		{
			assert(txn);
		}

		rocksdb_txn_rollback_guard(rocksdb_txn_rollback_guard const&) = delete;
		rocksdb_txn_rollback_guard& operator=(rocksdb_txn_rollback_guard const&) = delete;

		~rocksdb_txn_rollback_guard()
		{
			if (nullptr != txn_)
			{
				if (dismiss_)
					txn_->Rollback();

				delete txn_;
				txn_ = nullptr;
			}
		}

		void dismiss(bool dismiss = false) noexcept
		{
			dismiss_ = dismiss;
		}

		auto operator-> () noexcept
		{
			return txn_;
		}

		auto get() noexcept
		{
			return txn_;
		}

		rocksdb::Transaction*		txn_;
		bool						dismiss_ = true;
	};

	struct rocksdb_snapshot_guard
	{
		rocksdb_snapshot_guard(rocksdb::DB* db, rocksdb::Snapshot const*	snapshot)
			: db_(db)
			, snapshot_(snapshot)
		{
			assert(db);
			assert(snapshot);
		}

		~rocksdb_snapshot_guard()
		{
			if (snapshot_)
			{
				assert(db_);
				db_->ReleaseSnapshot(snapshot_);
				snapshot_ = nullptr;
			}
		}

		rocksdb::DB*				db_ = nullptr;
		rocksdb::Snapshot const*	snapshot_ = nullptr;
	};

	class queue_store
	{
		using value_type = uint32_t;
		//using counter_merge_operator = integral_merge_operator<value_type>;
		using queue_counter_t = queue_counter<value_type>;
		using column_family_handles_t = std::vector<rocksdb::ColumnFamilyHandle*>;
		
	public:
		explicit queue_store(std::string const& path)
		{
			init(path);
		}

		~queue_store()
		{
			if (db_)
			{
				for(auto handle : handles_)
				{
					if (handle)
						db_->DestroyColumnFamilyHandle(handle);
				}
			}
		}

		queue_store(queue_store const&) = delete;
		queue_store& operator= (queue_store const&) = delete;

		bool push_back(std::string const& topic, std::string const& value)
		{
			std::string topic_tail = topic + "_tail";

			auto txn_raw = db_->BeginTransaction(rocksdb::WriteOptions{});
			rocksdb_txn_rollback_guard txn = txn_raw;

			// get the tail index and lock
			value_type index;
			if (!queue_counter_t::get_for_update(txn.get(), topic_meta_handle_, topic_tail, index))
				return false;

			// update index
			if (!queue_counter_t::put(txn.get(), topic_meta_handle_, topic_tail, index + 1))
				return false;

			auto key = gen_(topic, index);
			rocksdb::Status s;
			// enqueue
			s = txn->Put(default_hanle_, key, value);
			if (!s.ok())
				return false;

			// commit 
			s = txn->Commit();
			if (!s.ok())
				return false;

			txn.dismiss();
			return true;
		}

		bool get_message(std::string const& topic, value_type index, std::string& value)
		{
			auto key = gen_(topic, index);
			auto s = db_->Get(rocksdb::ReadOptions{}, default_hanle_,  key, &value);
			return s.ok();
		}

		bool get_message(std::string const& topic, value_type begin, value_type end, std::string& value)
		{
			std::string topic_head_key = topic + "_head";
			std::string topic_tail_key = topic + "_tail";
			value_type head_index = 0, tail_index = 0;
			
			rocksdb::ReadOptions op;

			auto snapshot = db_->GetSnapshot();
			if (nullptr == snapshot)
				return false;
			
			rocksdb_snapshot_guard snapshort_guard = { db_.get(), snapshot };
			op.snapshot = snapshot;
	
			if (!queue_counter_t::get(db_, op, topic_meta_handle_, topic_head_key, head_index))
				return false;
			if (!queue_counter_t::get(db_, op, topic_meta_handle_, topic_tail_key, tail_index))
				return false;

			if (begin < head_index)
				begin = head_index;
			if (end > tail_index)
				end = tail_index;

			auto tail_key = gen_(topic, end);
			auto head_key = gen_(topic, begin);

			rocksdb::Slice upper_bound = tail_key;
			op.iterate_upper_bound = &upper_bound;

			auto itr_raw = db_->NewIterator(op, default_hanle_);
			if (nullptr == itr_raw)
				return false;

			std::unique_ptr<rocksdb::Iterator> itr{ itr_raw };

			value.push_back('[');
			itr->Seek(head_key);
			while (itr->Valid())
			{
				auto v = itr->value();
				value.append(v.data(), v.size());
				value.push_back(',');
				itr->Next();
			}
			if (value.size() > 1)
				value.back() = ']';
			else
				value.push_back(']');

			return true;
		}

	private:
		void init(std::string const& path)
		{
			init_db(path);
			open_db(path);
		}

		void init_db(std::string const& path)
		{
			using namespace rocksdb;
			Status s;

			DBOptions op;
			op.create_if_missing = true;

			// check db
			std::vector<std::string> column_families;
			DB::ListColumnFamilies(op, path, &column_families);
			auto is_to_create_topic_meta_column_family =
				std::find(column_families.begin(), column_families.end(),
					topic_meta_column_family_name_) == column_families.end();

			normal_db_t db;
			if (is_to_create_topic_meta_column_family)
			{
				DB* db_raw = nullptr;
				Options option;
				option.create_if_missing = true;
				s = DB::Open(option, path, &db_raw);
				if (Status::OK() != s)
				{
					assert(nullptr == db_raw);
					throw std::runtime_error{ s.getState() };
				}

				assert(db_raw);
				db.reset(db_raw);
				ColumnFamilyHandle* handle_raw = nullptr;
				s = db->CreateColumnFamily(ColumnFamilyOptions{},
					topic_meta_column_family_name_, &handle_raw);
				if (Status::OK() != s)
				{
					assert(nullptr == handle_raw);
					throw std::runtime_error{ s.getState() };
				}

				assert(handle_raw);
				db->DestroyColumnFamilyHandle(handle_raw);
			}
		}

		void open_db(std::string const& path)
		{
			using namespace rocksdb;
			Status s;

			Options op;
			op.IncreaseParallelism(std::thread::hardware_concurrency());
			op.OptimizeLevelStyleCompaction();
			op.create_if_missing = true;
			//op.merge_operator = std::make_shared<counter_merge_operator>();
			op.max_successive_merges = 5;

			TransactionDBOptions txn_op;

			// open DB with two column families
			std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
			// have to open default column family
			column_families.push_back(ColumnFamilyDescriptor(
				kDefaultColumnFamilyName, ColumnFamilyOptions{}));
			// open the new one, too
			column_families.push_back(ColumnFamilyDescriptor(
				topic_meta_column_family_name_, ColumnFamilyOptions{}));
			std::vector<ColumnFamilyHandle*> raw_handles;

			TransactionDB* db_raw = nullptr;
			s = TransactionDB::Open(op, txn_op, path, column_families, &raw_handles, &db_raw);
			if (Status::OK() != s)
			{
				assert(nullptr == db_raw);
				throw std::runtime_error{ s.getState() };
			}
			assert(db_raw);

			// cache db and handles with raii for exceptional safty
			transaction_db_t db{ db_raw };

			auto itr = std::find_if(raw_handles.begin(), raw_handles.end(),
				[this](auto const& handle)
			{
				return handle->GetName() == topic_meta_column_family_name_;
			});

			if (raw_handles.end() == itr)
			{
				throw std::runtime_error{ "Rocksdb status is not inconsistence." };
			}

			default_hanle_ = raw_handles[0];
			topic_meta_handle_ = *itr;
			handles_ = std::move(raw_handles);
			db_ = std::move(db);
		}

	private:
		transaction_db_t				db_;
		queue_generator const			gen_;
		std::string const				topic_meta_column_family_name_ = "topic_meta";
		column_family_handles_t		handles_;
		rocksdb::ColumnFamilyHandle*	topic_meta_handle_ = nullptr;
		rocksdb::ColumnFamilyHandle*	default_hanle_ = nullptr;
	};
}

/*#include <iostream>

int main()
{
	timax::queue_store queue_store("/home/coding/temp/queue_store");

	queue_store.push_back("test_topic", "{test1}");
	queue_store.push_back("test_topic", "{test2}");
	queue_store.push_back("test_topic", "{test3}");
	queue_store.push_back("test_topic", "{test4}");
	
	
	std::string messages;
	messages.reserve(100);
	queue_store.get_message("test_topic", 1, 5, messages);
	
	std::cout << messages << std::endl;

	std::string value;
	value.clear();
	queue_store.get_message("test_topic", 1, value);
	value.clear();
	queue_store.get_message("test_topic", 2, value);
	value.clear();
	queue_store.get_message("test_topic", 3, value);
	value.clear();
	queue_store.get_message("test_topic", 4, value);

	return 0;
}*/