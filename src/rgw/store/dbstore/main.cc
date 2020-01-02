#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <dbstore.h>
#include <pthread.h>
#include <dbstore-log.h>

#ifdef SQLITE_ENABLED
#include <sqliteDB.h>
#endif

struct thr_args {
	class DBstore *db;
	int thr_id;
};

void* process(void *arg)
{
	struct thr_args *t_args = (struct thr_args*)arg;

	class DBstore *db = t_args->db;
	int thr_id = t_args->thr_id;

	dout(L_EVENT)<<"Entered thread:"<<thr_id<<"\n";

	string user1 = "Soumya";
	string bucketa = "rgw";
	string objecta1 = "bugfixing";
	string objecta2 = "zipper";
	string bucketb = "gluster";
	string objectb1 = "bugfixing";
	string objectb2 = "delegations";

	string user2 = "Kasturi";
	string bucketc = "qe";
	string objectc1 = "rhhi";
	string objectc2 = "cns";

	struct RGWOpParams params = {};

	db->InitializeParams("InsertUser", &params);
	params.user_name = user1;
	db->ProcessOp("InsertUser", &params);

	params.bucket_name = bucketa;
	db->ProcessOp("InsertBucket", &params);

	params.object = objecta1;
	
	db->ProcessOp("InsertObject", &params);
	params.object = objecta2;
	db->ProcessOp("InsertObject", &params);

	params.bucket_name = bucketb;
	db->ProcessOp("InsertBucket", &params);

	params.object = objectb1;
	db->ProcessOp("InsertObject", &params);
	params.object = objectb2;
	db->ProcessOp("InsertObject", &params);

	params.offset = 0;
	params.data = "Hello World \0";
	params.datalen = params.data.length();
	db->ProcessOp("PutObjectData", &params);

	params.offset = 20;
	params.data = "Hi There \0";
	params.datalen = params.data.length();
	db->ProcessOp("PutObjectData", &params);

	db->ProcessOp("GetObjectData", &params);

	db->ProcessOp("DeleteObjectData", &params);

	db->ProcessOp("GetObjectData", &params);

	params.user_name = user2;
	db->ProcessOp("InsertUser", &params);

	params.bucket_name = bucketc;
	db->ProcessOp("InsertBucket", &params);

	params.object = objectc1;
	db->ProcessOp("InsertObject", &params);
	params.object = objectc2;
	db->ProcessOp("InsertObject", &params);

	db->ProcessOp("ListUser", &params);
	db->ProcessOp("ListBucket", &params);
	db->ProcessOp("ListObject", &params);

	db->ListAllUsers(&params);
	db->ListAllBuckets(&params);
	db->ListAllObjects(&params);

	params.object = objecta1;
	params.bucket_name = bucketa;
	db->ProcessOp("RemoveObject", &params);

	params.bucket_name = bucketb;
	db->ProcessOp("RemoveBucket", &params);

	params.user_name = user2;
	db->ProcessOp("RemoveUser", &params);

	db->ListAllUsers(&params);
	db->ListAllBuckets(&params);
	db->ListAllObjects(&params);

	dout(L_EVENT)<<"Exiting thread:"<<thr_id<<"\n";

	return 0;
}

int main(int argc, char *argv[])
{
	string tenant = "Redhat";
	string logfile;
	int loglevel;

	class DBstore *db;
	int rc = 0, tnum = 0;
	void *res;

	pthread_attr_t attr;
	int num_thr = 2;
	pthread_t threads[num_thr];
	struct thr_args t_args[num_thr];

	// format: ./dbstore logfile loglevel
	if (argc == 3) {
		logfile = argv[1];
		loglevel = (atoi)(argv[2]);
	}

#ifdef SQLITE_ENABLED
	db = new SQLiteDB(tenant);
#else
	db = new DBstore(tenant);
#endif

	rc = db->Initialize(logfile, loglevel);

	if (rc != 0) {
		cout<<"DBstore initialization failed for tenant:" \
			   <<tenant<<"\n";
		goto out;
	}

	dout(L_EVENT)<<"No. of threads being created = "<<num_thr<<"\n";

        /* Initialize thread creation attributes */
        rc = pthread_attr_init(&attr);

        if (rc != 0) {
             dout(L_ERR)<<" error in pthread_attr_init \n";
	     goto out;
	}


	for (tnum = 0; tnum < num_thr; tnum++) {
		t_args[tnum].db = db;
		t_args[tnum].thr_id = tnum;
        	rc = pthread_create((pthread_t*)&threads[tnum], &attr, &process,
			             &t_args[tnum]);
                if (rc != 0) {
        		dout(L_ERR)<<" error in pthread_create \n";
			goto out;
		}

		dout(L_FULLDEBUG)<<"Created thread (thread-id:"<<tnum<<")\n";
        }

        /* Destroy the thread attributes object, since it is no
           longer needed */

        rc = pthread_attr_destroy(&attr);
        if (rc != 0) {
        	dout(L_EVENT)<<"error in pthread_attr_destroy \n";
	}

        /* Now join with each thread, and display its returned value */

        for (tnum = 0; tnum < num_thr; tnum++) {
        	rc = pthread_join(threads[tnum], &res);
                if (rc != 0)
                	dout(L_ERR)<<"error in pthread_join \n";
		else
               		dout(L_EVENT)<<"Joined with thread "<<tnum<<"\n";
         }

out:
	db->Destroy();
	delete db;

	return 0;
}
