#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <curl\curl.h>

static char const post_file_buf[] = "Expect:";
static char const post_file_file_name[] = "file_name: ";

size_t const post_file_argc_size = 3;
size_t const post_file_name_index = 2;
size_t const post_file_url_index = 1;

std::string post_file(
	std::string const& url, 
	std::string const& file_name, 
	std::string const& content);

long recv_func(void* ptr, int size, int nmemb, void* data);

int main(int argc, char* argv[])
{
	if (post_file_argc_size != argc)
		std::cout << "USAGE: ./post_file.exe url file-name" << std::endl;

	std::string url = argv[post_file_url_index];
	std::string file_name = argv[post_file_name_index];

	std::ifstream file_stream;
	file_stream.open(file_name);
	if (!file_stream)
		std::cout << "File: " << file_name << " not exists!" << std::endl;

	std::string file_content;
	file_stream.seekg(0, std::ios::end);
	size_t size = file_stream.tellg();
	file_content.resize(size);
	file_stream.seekg(std::ios::beg);
	file_stream.read(&file_content[0], size);
	file_stream.close();

	std::cout << post_file(url, file_name, file_content) << std::endl;
	return 0;
}

struct curl_delete
{
	void operator() (CURL* curl)
	{
		if (nullptr != curl)
			curl_easy_cleanup(curl);
	}
};

struct curl_delete_slist
{
	void operator() (curl_slist* list)
	{
		if (nullptr != list)
			curl_slist_free_all(list);
	}
};

// 192.168.0.24/upload_file?file_name=1.gif

std::string post_file(
	std::string const& url, 
	std::string const& file_name, 
	std::string const& content)
{
	std::unique_ptr<CURL, curl_delete> curl;
	curl.reset(curl_easy_init());
	if (!curl)
		return "Failed to initialize curl!";

	// append headers
	curl_slist* chunck = nullptr;
	chunck = curl_slist_append(chunck, post_file_buf);
	
	// append file name
	std::string file_name_header;
	file_name_header = post_file_file_name + file_name;
	chunck = curl_slist_append(chunck, file_name_header.c_str());

	// raii
	std::unique_ptr<curl_slist, curl_delete_slist> headerlist;
	headerlist.reset(chunck);

	// response
	std::string response;

	// http options
	curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());				// url
	curl_easy_setopt(curl.get(), CURLOPT_VERBOSE, 1L);					// trace
	curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headerlist.get());	// append header
	curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, content.c_str());	// file content
	curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, recv_func);		// write function
	curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);			// write dest

	// perform curl
	CURLcode res = curl_easy_perform(curl.get());
	if (CURLE_OK != res)
		return curl_easy_strerror(res);

	return response;
}

long recv_func(void* ptr, int size, int nmemb, void* data)
{
	auto sizes = static_cast<long>(size * nmemb);
	auto& content = *reinterpret_cast<std::string*>(data);
	content.append(reinterpret_cast<char*>(ptr), sizes);
	return sizes;
}