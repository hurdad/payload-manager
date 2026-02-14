#include <pqxx/pqxx>

#if pqxx_have_source_location
namespace pqxx {
argument_error::argument_error(std::string const& whatarg, std::source_location where)
    : std::invalid_argument(whatarg), location(where) {}

conversion_error::conversion_error(std::string const& whatarg, std::source_location where)
    : std::domain_error(whatarg), location(where) {}
}  // namespace pqxx
#endif
