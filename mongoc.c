#include <mongoc.h>

extern void logHandler(mongoc_log_level_t log_level, const char *log_domain, const char *message, void *user_data);

void mongoc_cgo_init()
{
    mongoc_init();
    mongoc_log_set_handler(logHandler, 0);
}
