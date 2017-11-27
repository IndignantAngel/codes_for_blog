#pragma once

#include <string>
#include <memory>
#include <thread>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>				// for lexical cast
#include <boost/uuid/name_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <rocksdb/db.h>

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
		static constexpr size_t file_reserve_size = 48;

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
			// input file generation params
			std::stringstream ss;
			ss << major_name << timestamp << std::this_thread::get_id();

			// generate file
			std::string file_name;
			file_name.reserve(file_reserve_size);
			file_name = gen_(ss.str());

			// add ext
			auto pos = major_name.rfind('.');
			if (std::string::npos != pos)
			{
				file_name += major_name.substr(pos);
			}	

			return file_name;
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
}