/* networking proof of concept client */
#include <mongoc.h>
#include <stdio.h>
#include <time.h>

int main(int argc, char *argv[]) {
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_error_t error;
   int64_t start;
   int64_t end;
   bson_t selector;
   bson_t update;
   bson_t child;
   int num_ops;
   bool res;
   int i;

   if (argc == 2) {
      num_ops = atoi(argv[1]);
   } else {
      num_ops = 1000;
   }

   mongoc_init ();

   client = mongoc_client_new ("mongodb://127.0.0.1/");
   if (!client) {
      printf ("Failed to create client\n");
      return 0;
   }

   bson_init (&selector);
   bson_append_int32 (&selector, "a", -1, 1);

   bson_init (&update);
   bson_append_document_begin (&update, "$inc", -1, &child);
   bson_append_int32 (&child, "a", -1, 0);
   bson_append_document_end (&update, &child);

   collection = mongoc_client_get_collection (client, "test-networking", "poc");

   start = bson_get_monotonic_time ();

   for (i = 0; i < num_ops; i++) {
      res = mongoc_collection_update (collection, MONGOC_UPDATE_NONE, &selector, &update, NULL, &error);
      if (!res) {
         printf ("Operation failed.\n");
         return 0;
      }
   };

   printf ("Running %d updates took %f milliseconds\n", num_ops, (double)(bson_get_monotonic_time () - start)/1000);
}
