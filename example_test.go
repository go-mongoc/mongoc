package mongoc_test

import bson "gopkg.in/bson.v2"
import "gopkg.in/mongoc.v1"
import "fmt"

type User struct {
	ID       string `bson:"_id"`
	Username string `bson:"username"`
	Password string `bson:"password"`
}

func ExamplePool() {
	//
	//setup
	pool := mongoc.NewPool("mongodb://loc.m:27017", 100, 1)
	col := pool.C("test", "mongoc") //get collection.
	col.Remove(nil, false)          //clear data, not need.
	//
	//CRUD
	{
		//
		//insert
		err := col.Insert(
			&User{
				ID:       bson.NewObjectId().Hex(),
				Username: "user1",
				Password: "12345",
			},
			&User{
				ID:       bson.NewObjectId().Hex(),
				Username: "user2",
				Password: "12345",
			},
		)
		if err != nil {
			panic(err)
		}
		//
		//find
		users := []*User{}
		err = col.Find(
			bson.M{
				"username": "user1",
				"password": "12345",
			}, nil, 0, 0, &users)
		if err != nil {
			panic(err)
		}
		if len(users) != 1 {
			panic("not found")
		}
		fmt.Println(users[0].Username, "found")
		//
		//update
		err = col.Update(
			bson.M{
				"username": "user2",
				"password": "12345",
			}, bson.M{
				"$set": bson.M{
					"password": "54321",
				},
			}, false, false)
		if err != nil {
			panic(err)
		}
		users = []*User{}
		err = col.Find(
			bson.M{
				"username": "user2",
				"password": "54321",
			}, nil, 0, 0, &users)
		if err != nil {
			panic(err)
		}
		if len(users) != 1 {
			panic("not found")
		}
		fmt.Println(users[0].Username, "found")
		//
		// remove
		err = col.Remove(
			bson.M{
				"username": "user2",
				"password": "54321",
			}, true)
		if err != nil {
			panic(err)
		}
		users = []*User{}
		err = col.Find(
			bson.M{
				"username": "user2",
				"password": "54321",
			}, nil, 0, 0, &users)
		if err != nil {
			panic(err)
		}
		if len(users) != 0 {
			panic("not found")
		}
		fmt.Println("user2 removed")
	}
	//
	//Create index
	{
		//create index xxx on test.mongoc.
		//for more seed mongodb craeteIndexes command.
		err := pool.Execute("test", bson.D{
			{
				Name:  "createIndexes",
				Value: "mongoc",
			},
			{
				Name: "indexes",
				Value: []bson.M{
					bson.M{
						"name": "xxx",
						"key": bson.M{
							"xxx": 1,
						},
					},
				},
			},
		}, nil, &bson.M{})
		if err != nil {
			panic(err)
		}
		fmt.Println("index created")
	}
	// Output:
	// user1 found
	// user2 found
	// user2 removed
	// index created
}
