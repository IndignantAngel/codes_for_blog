#pragma once

#include <memory>
#include <thread>
#include <string>
#include <atomic>
#include <iostream>
#include <map>
#include <fstream>
#include <shared_mutex>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/name_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>

namespace timax
{
	class file_name_generator
	{
	public:
		file_name_generator()
			: seed_(boost::uuids::string_generator{}("c6c697f3-ca98-420b-bdbf-d4390ac025cf"))
		{
		}

		std::string operator() (std::string const& oprd) const
		{
			// create generator
			boost::uuids::name_generator gen{ seed_ };
			return boost::lexical_cast<std::string>(gen(oprd));
		}

	private:
		boost::uuids::uuid const			seed_;
	};

	class file_store
	{
	public:
		explicit file_store(std::string const& path)
		{
			init(path);
		}

		void put(std::string const& key, std::string const& value)
		{
			auto s = db_->Put(rocksdb::WriteOptions{}, key, value);
			if (!s.ok())
				throw std::runtime_error{ s.getState() };
		}

		std::string get(std::string const& key)
		{
			std::string value;
			auto s = db_->Get(rocksdb::ReadOptions{}, key, &value);
			if (!s.ok())
				throw std::runtime_error{ s.getState() };
			return value;
		}

		std::string generator_file_name(std::string const& major_name, std::string const& timestamp) const
		{
			std::stringstream ss;
			ss << major_name << timestamp << std::this_thread::get_id();
			return gen_(ss.str());
		}

	private:
		void init(std::string const& path)
		{
			rocksdb::Status s;

			rocksdb::Options op;
			op.IncreaseParallelism(std::thread::hardware_concurrency());
			op.OptimizeLevelStyleCompaction();
			op.create_if_missing = true;
			op.compression_per_level.resize(2);

			rocksdb::DB* db_raw = nullptr;
			s = rocksdb::DB::Open(op, path, &db_raw);
			if (rocksdb::Status::OK() != s)
			{
				throw std::runtime_error{ s.getState() };
			}

			db_.reset(db_raw);
		}

	private:
		std::unique_ptr<rocksdb::DB>		db_;
		file_name_generator				gen_;
	};

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

	struct topic_meta
	{
		uint32_t	begin;
		uint32_t	end;
	};

	struct queue_meta
	{
		std::atomic<uint32_t> begin;
		std::atomic<uint32_t> end;
	};

	class queue_store
	{
		// types
		using db_t = std::unique_ptr<rocksdb::DB>;
		using column_family_handle_t = std::unique_ptr<rocksdb::ColumnFamilyHandle>;
		using column_family_handles_t = std::vector<column_family_handle_t>;
		using iterator_t = std::unique_ptr<rocksdb::Iterator>;
		using queue_meta = std::map<std::string, queue_meta>;
		using read_lock_t = std::shared_lock<std::shared_mutex>;
		using write_lock_t = std::unique_lock<std::shared_mutex>;

		// const
	public:
		explicit queue_store(std::string const& path)
		{
			init(path);
			init_topic();
		}

		queue_store(queue_store const&) = delete;
		queue_store& operator= (queue_store const&) = delete;

		~queue_store()
		{
			// close column family handle first
			handles_.clear();
		}

		void push_back(std::string const& topic, std::string const& value)
		{
			//auto key = gen_(topic, value);
			auto itr = db_->NewIterator(rocksdb::ReadOptions{});
			//itr->Seek()
		}

	private:
		void init(std::string const& path)
		{
			pre_init(path);
			open_db(path);
			init_topic();
		}

		void pre_init(std::string const& path)
		{
			using namespace rocksdb;
			Status s;

			// check db
			std::vector<std::string> column_families;
			DB::ListColumnFamilies(DBOptions{}, path, &column_families);
			auto is_to_create_topic_meta_column_family =
				std::find(column_families.begin(), column_families.end(),
					topic_meta_column_family_name_) == column_families.end();

			db_t db;
			if (is_to_create_topic_meta_column_family)
			{
				DB* db_raw = nullptr;
				s = DB::Open(Options{}, path, &db_raw);
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
				column_family_handle_t handle{ handle_raw };
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
			op.compression_per_level.resize(2);

			// open DB with two column families
			std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
			// have to open default column family
			column_families.push_back(ColumnFamilyDescriptor(
				kDefaultColumnFamilyName, ColumnFamilyOptions{}));
			// open the new one, too
			column_families.push_back(ColumnFamilyDescriptor(
				topic_meta_column_family_name_, ColumnFamilyOptions{}));
			std::vector<ColumnFamilyHandle*> raw_handles;

			column_family_handles_t handles;
			handles.reserve(column_families.size() + 1);

			DB* db_raw = nullptr;
			s = DB::Open(op, path, column_families, &raw_handles, &db_raw);
			if (Status::OK() != s)
			{
				assert(nullptr == db_raw);
				throw std::runtime_error{ s.getState() };
			}
			assert(db_raw);

			// cache db and handles with raii for exceptional safty
			db_t db{ db_raw };
			for (auto raw_handle : raw_handles)
			{
				assert(raw_handle);
				handles.emplace_back(raw_handle);
			}

			auto itr = std::find_if(handles.begin(), handles.end(),
				[this](auto const& handle)
			{
				return handle->GetName() == topic_meta_column_family_name_;
			});

			if (handles.end() == itr)
			{
				throw std::runtime_error{ "Rocksdb status is not inconsistence." };
			}

			topic_meta_handle_ = itr->get();
			handles_ = std::move(handles);
			db_ = std::move(db);
		}

		void init_topic()
		{
			assert(topic_meta_handle_);
			iterator_t itr{
				db_->NewIterator(rocksdb::ReadOptions{}, topic_meta_handle_)
			};

			while (itr->Valid())
			{
				std::string topic = itr->key().ToString();
				//std::string value = 
			}
		}

	private:
		db_t							db_;
		queue_generator const			gen_;
		queue_meta					meta_;
		std::shared_mutex				mutex_;
		std::string const				topic_meta_column_family_name_ = "topic_meta";
		column_family_handles_t		handles_;
		rocksdb::ColumnFamilyHandle*	topic_meta_handle_ = nullptr;
	};
}