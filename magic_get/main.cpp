// requires: C++14
#include <iostream>
#include <string>

//#include "boost/pfr/precise.hpp"
//
//struct some_person {
//    std::string name;
//    unsigned birth_year;
//};

namespace detail
{
	//template <size_t>
	//auto create_param()
	//{
	//    return boost::pfr::detail::ubiq_lref_constructor{};
	//}
	//
	//template <typename T, size_t ... Is>
	//auto test_construct(std::index_sequence<Is...>)
	//    -> std::en
	//
	//template <typename T, typename Indics, typename = void>
	//struct is_reflectable_impl : std::false_type
	//{
	//};
	//
	//template <typename T, size_t ... Is>
	//struct is_reflectable_impl<T, std::index_sequence<Is...>,
}

//template <typename T>
//struct is_reflectable
//{
//    constexpr static size_t field_count = boost::pfr::detail::fields_count<some_person>();
//};

namespace pfr
{
	template <class T, std::size_t N>
	struct tag {
		friend auto loophole(tag<T, N>);
	};
	
	template <class T, class U, std::size_t N, bool B>
	struct fn_def {
		friend auto loophole(tag<T, N>) { return U{}; }
	};
	
	// This specialization is to avoid multiple definition errors.
	template <class T, class U, std::size_t N>
	struct fn_def<T, U, N, true> {};
	
	template <class T, std::size_t N>
	struct loophole_ubiq {
		template<class U, std::size_t M> static std::size_t ins(...);
		template<class U, std::size_t M, std::size_t = sizeof(loophole(tag<T, M>{})) > static char ins(int);
		
		template<class U, std::size_t = sizeof(fn_def<T, U, N, sizeof(ins<U, N>(0)) == sizeof(char)>)>
		constexpr operator U&() const noexcept; // `const` here helps to avoid ambiguity in loophole instantiations. optional_like test validate that behavior.
	};
	
}


// adl lookup

struct foo
{
	std::string a;
	double b;
};


int main() {
	
	using type1 = decltype(static_cast<std::string&>(pfr::loophole_ubiq<foo, 0>{}));
	using type2 = decltype(static_cast<double&>(pfr::loophole_ubiq<foo, 1>{}));
	
	auto r = std::is_same<std::string, decltype(loophole(pfr::tag<foo, 0>{}))>::value;
	r = std::is_same<double, decltype(loophole(pfr::tag<foo, 1>{}))>::value;
}