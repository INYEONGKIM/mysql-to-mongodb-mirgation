import pymysql
import pymongo
from time import perf_counter
from collections import deque
from pyfiglet import Figlet


class MySQLtoMongo:
    def __init__(self, mysql_id, mysql_pw, mysql_db_name, mongo_id, mongo_pw, mongo_db_name):
        self.mysql_id = mysql_id
        self.mysql_pw = mysql_pw
        self.mysql_db_name = mysql_db_name

        self.mongo_id = mongo_id
        self.mongo_pw = mongo_pw
        self.mongo_db_name = mongo_db_name

        self.defalut_max_len = 9
        self.print_mysql_mode = False

    def clear_collections(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        for collection_name in ("department", "time_slot", "takes", "student", "section", "instructor", "course"):
            collection = mongodb[collection_name]
            collection.drop()

    def init_tables_to_collection(self):
        # clear first
        self.clear_collections()

        # make collections
        self.make_department_collection() # 20
        self.make_time_slot_collection() # 20
        self.make_takes_collection() # 30000

        self.make_student_collection() # 2000 (student + advisor)
        self.make_section_collection() # 100 (section + classroom)

        self.make_instructor_collection() # 119 -> 50 (instructor + teaches)
        self.make_course_collection() # 221 -> 200 (course + prereq)

        print("Fin init!")

    def make_department_collection(self):
        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["department"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT * FROM department;'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] department : INSERT {len(result)} TUPLES!")

                for res_tuple in result:
                    dept_name, building, budget = res_tuple
                    collection.insert_one({
                        "dept_name": dept_name,
                        "building": building,
                        "budget": float(budget)
                    })

        finally:
            conn.close()
            db.close()
            print("[FIN] department\n")

    def make_time_slot_collection(self):
        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["time_slot"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT * FROM time_slot;'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] time_slot : INSERT {len(result)} TUPLES!")

                for res_tuple in result:
                    time_slot_id, day, start_hr, start_min, end_hr, end_min = res_tuple
                    collection.insert_one({
                        "time_slot_id": time_slot_id,
                        "day": day,
                        "start_hr": int(start_hr),
                        "start_min": int(start_min),
                        "end_hr": int(end_hr),
                        "end_min": int(end_min)
                    })

        finally:
            conn.close()
            db.close()
            print("[FIN] time_slot\n")

    def make_takes_collection(self):
        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["takes"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT * FROM takes;'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] takes : INSERT {len(result)} TUPLES!")

                for res_tuple in result:
                    ID, course_id, sec_id, semester, year, grade = res_tuple
                    collection.insert_one({
                        "ID": ID,
                        "course_id": course_id,
                        "sec_id": sec_id,
                        "semester": semester,
                        "year": int(year),
                        "grade": grade
                    })

        finally:
            conn.close()
            db.close()
            print("[FIN] takes\n")

    def make_student_collection(self):
        # student + advisor (1:1)
        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["student"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT s.ID, s.name, s.dept_name, s.tot_cred, a.i_ID ' \
                      'FROM student as s LEFT JOIN advisor as a ON s.ID=a.s_ID;'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] student : INSERT {len(result)} TUPLES!")

                for res_tuple in result:
                    ID, name, dept_name, tot_cred, advisor = res_tuple
                    collection.insert_one({
                        "ID": ID,
                        "name": name,
                        "dept_name": dept_name,
                        "tot_cred": int(tot_cred),
                        "advisor": advisor
                    })

        finally:
            conn.close()
            db.close()
            print("[FIN] student\n")

    def make_section_collection(self):
        # section + classroom (*:1)
        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["section"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT s.course_id, s.sec_id, s.semester, s.year, s.building, s.room_number, s.time_slot_id, c.capacity ' \
                      'FROM section as s LEFT JOIN classroom as c ' \
                      'ON (s.building=c.building and s.room_number=c.room_number);'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] section : INSERT {len(result)} TUPLES!")

                for res_tuple in result:
                    course_id, sec_id, semester, year, building, room_number, time_slot_id, capacity = res_tuple
                    collection.insert_one({
                        "course_id": course_id,
                        "sec_id": sec_id,
                        "semester": semester,
                        "year": int(year),
                        "building": building,
                        "room_number": room_number,
                        "time_slot_id": time_slot_id,
                        "capacity": int(capacity)
                    })

        finally:
            conn.close()
            db.close()
            print("[FIN] section\n")

    def make_instructor_collection(self):
        # instructor + teaches (1:*)

        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["instructor"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT i.ID, i.name, i.dept_name, i.salary, t.course_id, t.sec_id, t.semester, t.year ' \
                      'FROM instructor as i LEFT JOIN teaches as t ON i.ID=t.ID ORDER BY i.ID;'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] instructor : INSERT {len(result)} TUPLES!")

                pre_instructor_id = ""
                pre_name = ""
                pre_dept_name = ""
                pre_salary = -1

                teaches_list = []

                tot_len = len(result)

                for idx, res_tuple in enumerate(result):
                    instructor_id, name, dept_name, salary, course_id, sec_id, semester, year = res_tuple

                    # set pre data
                    if idx == 0:
                        pre_instructor_id, pre_name, pre_dept_name, pre_salary = instructor_id, name, dept_name, float(salary)

                        if course_id is None:
                            collection.insert_one({
                                "ID": instructor_id,
                                "name": name,
                                "dept_name": dept_name,
                                "salary": float(salary),
                                "teaches": []
                            })

                        else:
                            teaches_list.append({
                                "course_id": course_id, "sec_id": sec_id, "semester": semester, "year": int(year)
                            })

                    elif idx == tot_len - 1:
                        if course_id is None:

                            # process before list
                            if teaches_list:
                                collection.insert_one({
                                    "ID": pre_instructor_id,
                                    "name": pre_name,
                                    "dept_name": pre_dept_name,
                                    "salary": pre_salary,
                                    "teaches": teaches_list
                                })

                            # insert now one
                            collection.insert_one({
                                "ID": instructor_id,
                                "name": name,
                                "dept_name": dept_name,
                                "salary": float(salary),
                                "teaches": []
                            })

                        else:
                            if pre_instructor_id == instructor_id:
                                teaches_list.append({
                                    "course_id": course_id, "sec_id": sec_id, "semester": semester, "year": int(year)
                                })

                                collection.insert_one({
                                    "ID": pre_instructor_id,
                                    "name": pre_name,
                                    "dept_name": pre_dept_name,
                                    "salary": pre_salary,
                                    "teaches": teaches_list
                                })

                            else:
                                # process pre data
                                collection.insert_one({
                                    "ID": pre_instructor_id,
                                    "name": pre_name,
                                    "dept_name": pre_dept_name,
                                    "salary": pre_salary,
                                    "teaches": teaches_list
                                })

                                # insert now one
                                collection.insert_one({
                                    "ID": instructor_id,
                                    "name": name,
                                    "dept_name": dept_name,
                                    "salary": float(salary),
                                    "teaches": [{
                                        "course_id": course_id, "sec_id": sec_id, "semester": semester, "year": int(year)
                                    }]
                                })

                    else:
                        if course_id is None:
                            # process before list
                            if teaches_list:
                                collection.insert_one({
                                    "ID": pre_instructor_id,
                                    "name": pre_name,
                                    "dept_name": pre_dept_name,
                                    "salary": pre_salary,
                                    "teaches": teaches_list
                                })

                            pre_instructor_id, pre_name, pre_dept_name, pre_salary, teaches_list = instructor_id, name, dept_name, float(salary), []

                            collection.insert_one({
                                "ID": instructor_id,
                                "name": name,
                                "dept_name": dept_name,
                                "salary": float(salary),
                                "teaches": []
                            })

                        else:
                            if pre_instructor_id == instructor_id:
                                teaches_list.append({
                                    "course_id": course_id, "sec_id": sec_id, "semester": semester, "year": int(year)
                                })

                            else:
                                # process before list
                                if teaches_list:
                                    collection.insert_one({
                                        "ID": pre_instructor_id,
                                        "name": pre_name,
                                        "dept_name": pre_dept_name,
                                        "salary": pre_salary,
                                        "teaches": teaches_list
                                    })

                                teaches_list = [{
                                    "course_id": course_id, "sec_id": sec_id, "semester": semester, "year": int(year)
                                }]

                                pre_instructor_id, pre_name, pre_dept_name, pre_salary = instructor_id, name, dept_name, float(salary)

        finally:
            conn.close()
            db.close()
            print("[FIN] instructor\n")

    def make_course_collection(self):

        db = pymysql.connect(host='localhost', port=3306, user=self.mysql_id, passwd=self.mysql_pw,
                             db=self.mysql_db_name, charset='utf8')

        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        collection = mongodb["course"]

        try:
            with db.cursor() as cursor:
                sql = 'SELECT c.course_id, c.title, c.dept_name, c.credits, p.prereq_id ' \
                      'FROM course as c LEFT JOIN prereq as p ON c.course_id=p.course_id ORDER BY c.course_id;'
                cursor.execute(sql)
                result = cursor.fetchall()

                print(f"[START] course : INSERT {len(result)} TUPLES!")

                pre_course_id = ""
                pre_title = ""
                pre_dept_name = ""
                pre_credits = -1

                prereq_list = []

                tot_len = len(result)

                for idx, res_tuple in enumerate(result):
                    course_id, title, dept_name, c_credits, prereq = res_tuple

                    # set pre data
                    if idx == 0:
                        pre_course_id, pre_title, pre_dept_name, pre_credits = course_id, title, dept_name, int(c_credits)

                        if prereq is None:
                            collection.insert_one({
                                "course_id": course_id,
                                "title": title,
                                "dept_name": dept_name,
                                "credits": int(c_credits),
                                "prereq": []
                            })

                        else:
                            prereq_list.append({"prereq_id": prereq})

                    elif idx == tot_len - 1:
                        if prereq is None:

                            # process before list
                            if prereq_list:
                                collection.insert_one({
                                    "course_id": pre_course_id,
                                    "title": pre_title,
                                    "dept_name": pre_dept_name,
                                    "credits": pre_credits,
                                    "prereq": prereq_list
                                })

                            # insert now one
                            collection.insert_one({
                                "course_id": course_id,
                                "title": title,
                                "dept_name": dept_name,
                                "credits": int(c_credits),
                                "prereq": []
                            })

                        else:
                            if pre_course_id == course_id:
                                prereq_list.append({"prereq_id": prereq})

                                collection.insert_one({
                                    "course_id": pre_course_id,
                                    "title": pre_title,
                                    "dept_name": pre_dept_name,
                                    "credits": pre_credits,
                                    "prereq": prereq_list
                                })

                            else:
                                # process pre data
                                collection.insert_one({
                                    "course_id": pre_course_id,
                                    "title": pre_title,
                                    "dept_name": pre_dept_name,
                                    "credits": pre_credits,
                                    "prereq": prereq_list
                                })

                                # insert now one
                                collection.insert_one({
                                    "course_id": course_id,
                                    "title": title,
                                    "dept_name": dept_name,
                                    "credits": int(c_credits),
                                    "prereq": [{"prereq_id": prereq}]
                                })

                    else:
                        if prereq is None:
                            # process before list
                            if prereq_list:
                                collection.insert_one({
                                    "course_id": pre_course_id,
                                    "title": pre_title,
                                    "dept_name": pre_dept_name,
                                    "credits": pre_credits,
                                    "prereq": prereq_list
                                })

                            pre_course_id, pre_title, pre_dept_name, pre_credits, prereq_list = course_id, title, dept_name, int(c_credits), []
                            collection.insert_one({
                                "course_id": course_id,
                                "title": title,
                                "dept_name": dept_name,
                                "credits": int(c_credits),
                                "prereq": []
                            })

                        else:
                            if pre_course_id == course_id:
                                prereq_list.append({"prereq_id": prereq})

                            else:
                                # process before list
                                if prereq_list:
                                    collection.insert_one({
                                        "course_id": pre_course_id,
                                        "title": pre_title,
                                        "dept_name": pre_dept_name,
                                        "credits": pre_credits,
                                        "prereq": prereq_list
                                    })

                                prereq_list = [{"prereq_id": prereq}]
                                pre_course_id, pre_title, pre_dept_name, pre_credits = course_id, title, dept_name, int(c_credits)

            # add index on dept_name
            collection.create_index([("dept_name", pymongo.DESCENDING)])

        finally:
            conn.close()
            db.close()
            print("[FIN] course\n")

    # TODO : Query 1 (FIN)
    def find_query1(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        start = perf_counter()

        collection = mongodb["instructor"]

        result = collection.find({
            "$and": [
                {"dept_name": "Comp. Sci."},
                {"salary": {"$gt": 80000}}
            ]
        }, {
            "_id": 0,
            "name": 1
        })
        finish = perf_counter()

        query_time = round(finish - start, 4)

        tot = 0
        if self.print_mysql_mode:
            title_list = deque(["name"])
            contents_list = deque()

            max_len = self.defalut_max_len
            for title in title_list:
                max_len = max(max_len, len(title))

            for res_tuple in result:
                tot += 1
                contents_list.append([res_tuple["name"]])
                max_len = max(max_len, len(res_tuple["name"]))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len + 1)

        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

        conn.close()

    # TODO : Query 2 (FIN)
    def find_query2(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        start = perf_counter()

        collection = mongodb["instructor"]

        result = collection.aggregate([
            {"$unwind": "$teaches"},
            {
                "$project": {
                    "_id": 0,
                    "ID": 0,
                    "dept_name": 0,
                    "salary": 0,
                    "teaches.sec_id": 0,
                    "teaches.semester": 0,
                    "teaches.year": 0
                }
            }
        ])

        finish = perf_counter()

        query_time = round(finish - start, 4)

        tot = 0

        if self.print_mysql_mode:
            title_list = deque(["name", "course_id"])
            contents_list = deque()

            max_len = self.defalut_max_len
            for title in title_list:
                max_len = max(max_len, len(title))

            for res_tuple in result:
                tot += 1
                contents_list.append([res_tuple["name"], res_tuple["teaches"]["course_id"]])
                max_len = max(max_len, len(res_tuple["name"]), len(res_tuple["teaches"]["course_id"]))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len+1)

        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

        conn.close()

    # TODO : Query 3 (FIN)
    def find_query3(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        start = perf_counter()

        collection = mongodb["instructor"]

        result = collection.aggregate([
            {"$unwind": "$teaches"},
            {
                "$lookup": {
                    "from": "course",
                    "let": {
                        "c_id": "$teaches.course_id",
                        "dept": "$dept_name"
                    },
                    "pipeline": [
                        {
                            "$match": {"$expr": {
                                "$and": [
                                    {"$eq": ["$course_id", "$$c_id"]},
                                    {"$eq": ["$dept_name", "$$dept"]}
                                ]
                            }}
                        }
                    ],
                    "as": "sub_course"
                }
            },
            {
                "$project":
                    {
                        "_id": 0,
                        "ID": 0,
                        "salary": 0,
                        "dept_name": 0,
                        "teaches": 0,

                        "sub_course._id": 0,
                        "sub_course.course_id": 0,
                        "sub_course.dept_name": 0,
                        "sub_course.credits": 0,
                        "sub_course.prereq": 0
                    }
            },
            {"$unwind": "$sub_course"}
        ])

        finish = perf_counter()

        query_time = round(finish - start, 4)

        tot = 0

        if self.print_mysql_mode:
            title_list = deque(["name", "title"])
            contents_list = deque()

            max_len = self.defalut_max_len
            for title in title_list:
                max_len = max(max_len, len(title))

            for res_tuple in result:
                tot += 1
                contents_list.append([res_tuple["name"], res_tuple["sub_course"]["title"]])
                max_len = max(max_len, len(res_tuple["name"]), len(res_tuple["sub_course"]["title"]))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len + 1)
        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

        conn.close()

    # TODO : Query 4 (FIN)
    def find_query4(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        # TODO : make mongo query
        collection = mongodb["section"]
        start = perf_counter()
        result = collection.aggregate([
        {
            "$group": {
                '_id': "$course_id",
                'sem': {"$addToSet": "$semester"},
                'y': {"$addToSet": "$year"}
            }
        }, {
            "$match": {
                "$and": [{
                    "$and": [{"sem": "Fall", "y": 2006}]
                }, {
                    "$and": [{"sem": "Spring", "y": 2008}]
                }]
            }
        }])

        finish = perf_counter()

        query_time = round(finish - start, 4)

        tot = 0

        if self.print_mysql_mode:
            title_list = deque(["course_id"])
            contents_list = deque()

            max_len = self.defalut_max_len
            for title in title_list:
                max_len = max(max_len, len(title))

            for res_tuple in result:
                tot += 1
                contents_list.append([res_tuple["_id"]])
                max_len = max(max_len, len(res_tuple["_id"]))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len + 1)
        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

        conn.close()

    # TODO : Query 5 (FIN)
    def find_query5(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        start = perf_counter()

        collection = mongodb["instructor"]

        result = collection.aggregate([
            {
                "$group": {
                    '_id': {
                        "dept_name": "$dept_name",
                    },
                    'avg(salary)': {
                        '$avg': '$salary'
                    }
                }
            }
        ])

        finish = perf_counter()

        query_time = round(finish - start, 4)
        tot = 0

        if self.print_mysql_mode:
            title_list = deque(["dept_name", "avg(salary)"])
            contents_list = deque()

            max_len = self.defalut_max_len

            for res_tuple in result:
                tot += 1
                contents_list.append([res_tuple["_id"]["dept_name"], str(round(float(res_tuple["avg(salary)"]), 6))])
                max_len = max(max_len, len(res_tuple["_id"]["dept_name"]), len(str(round(float(res_tuple["avg(salary)"]), 6))))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len + 1)
        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

    # TODO : Easter egg
    def find_query6(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        mongodb = conn[self.mongo_db_name]

        start = perf_counter()

        collection = mongodb["student"]

        result = collection.aggregate([{
            '$match': {'advisor': '97302'}
        }, {
            "$lookup": {
                "from": "instructor",
                "localField": "advisor",
                "foreignField": "ID",
                "as": "adv"
            }
        }, {
            "$project": {
                "_id": 0,
                "name": 1,
                "adv.name": 1
            }
        }])

        finish = perf_counter()

        query_time = round(finish - start, 4)
        tot = 0

        if self.print_mysql_mode:
            title_list = deque(["instructor_name", "student_name"])
            contents_list = deque()

            max_len = self.defalut_max_len
            for title in title_list:
                max_len = max(max_len, len(title))

            for res_tuple in result:
                tot += 1

                contents_list.append([res_tuple["adv"][0]["name"], res_tuple["name"]])
                max_len = max(max_len, len(res_tuple["name"]), len(res_tuple["adv"][0]["name"]))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len + 1)
        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

    # TODO : Easter egg
    def find_query7(self):
        conn = pymongo.MongoClient(f"mongodb://{self.mongo_id}:{self.mongo_pw}@localhost:27017")

        start = perf_counter()

        mongodb = conn[self.mongo_db_name]

        i_collection = mongodb["instructor"]

        result = i_collection.aggregate([
            {"$unwind": "$teaches"},
            {
                "$lookup": {
                    "from": "section",
                    "let": {
                        "c_id": "$teaches.course_id",
                        "s_id": "$teaches.sec_id",
                        "sem": "$teaches.semester",
                        "year": "$teaches.year"
                    },
                    "pipeline": [
                        {
                            "$match": {"$expr": {
                                "$and": [
                                    {"$eq": ["$course_id", "$$c_id"]},
                                    {"$eq": ["$sec_id", "$$s_id"]},
                                    {"$eq": ["$semester", "$$sem"]},
                                    {"$eq": ["$year", "$$year"]},
                                    {"$eq": ["Taylor", "$building"]}
                                ]
                            }}
                        }
                    ],
                    "as": "sub_course"
                }
            },
            {
                "$project":
                    {
                        "_id": 0,
                        "ID": 0,
                        "salary": 0,
                        "dept_name": 0,
                        "teaches": 0,

                        "sub_course._id": 0,
                        "sub_course.sec_id": 0,
                        "sub_course.semester": 0,
                        "sub_course.year": 0,
                        "sub_course.time_slot_id": 0,
                        "sub_course.capacity": 0
                    }
            },
            {"$unwind": "$sub_course"}
        ])

        ##
        finish = perf_counter()

        query_time = round(finish - start, 4)

        tot = 0

        if self.print_mysql_mode:
            title_list = deque(["building", "instructor_name"])
            contents_list = deque()

            max_len = self.defalut_max_len
            for title in title_list:
                max_len = max(max_len, len(title))

            for res_tuple in result:
                tot += 1
                contents_list.append([res_tuple["sub_course"]["building"], res_tuple["name"]])
                max_len = max(max_len, len(res_tuple["sub_course"]["building"]), len(res_tuple["name"]))

            self.print_mysql_mode_result(title_list=title_list, contents_list=contents_list, tot=tot,
                                         query_time=query_time, max_len=max_len + 1)
        else:
            for res_tuple in result:
                tot += 1
                print(res_tuple)

            print(f"{tot} rows in the set ({query_time} sec)")

        conn.close()


    @staticmethod
    def print_mysql_mode_result(title_list, contents_list, tot, query_time, max_len):
        if len(contents_list) > 0:
            boundary_str = "+" + ("-" * (max_len + 1) + "+") * len(title_list) + "\n"

            res_str = boundary_str

            for title in title_list:
                res_str += f"| {title}" + " " * (max_len - len(title))
            res_str += "|\n" + boundary_str

            for content in contents_list:
                for x in content:
                    res_str += f"| {x}" + " " * (max_len - len(x))

                res_str += "|\n"
            res_str += boundary_str

            print(f"{res_str}\n{tot} rows in the set ({query_time} sec)")

        else:
            print(f"Empty set ({query_time})")


if __name__ == '__main__':

    migrator = MySQLtoMongo(mysql_id="ID", mysql_pw="PW", mysql_db_name="db_name",
                            mongo_id="ID", mongo_pw="PW", mongo_db_name="db_name")

    f = Figlet(font="slant")
    print(f.renderText("Hwan Ii"))
    print(f.renderText("Young Ki"))

    print("Welcome to Hwan Young Il Ki's RDB to MongoDB Migrator :)")

    while True:
        print("\n[i] Init MYSQL to MongoDB\t[1 ~ 5] Query 1 ~ 5 [c] Change Print Mode\t[Q] Quit\nPress to continue...")
        user_input = str(__import__('sys').stdin.readline())[:1]

        if user_input == 'q':
            print("Bye!")
            break

        elif user_input == '1':
            print("[Start Query 1]")
            migrator.find_query1()

        elif user_input == '2':
            print("[Start Query 2]")
            migrator.find_query2()

        elif user_input == '3':
            print("[Start Query 3]")
            migrator.find_query3()

        elif user_input == '4':
            print("[Start Query 4]")
            migrator.find_query4()

        elif user_input == '5':
            print("[Start Query 5]")
            migrator.find_query5()

        elif user_input == '6':
            migrator.find_query6()

            print("\n[Query 6] You Found Easter Egg!\nOrigin SQL = \n\t"
                  "select i.name, n.name from instructor as i "
                  "left join (select s.name, a.i_id "
                  "from student as s "
                  "left join advisor as a on s.ID=a.s_id) as n on i.ID=n.i_id where i.name='Bertolino';")

        elif user_input == '7':
            migrator.find_query7()
            print("\n[Query 7] You Found Easter Egg!\nOrigin SQL = \n\t"
                  "select inst.name, class.building, class.room_number from \n\t"
                  "(select i.ID, i.name, t.course_id, t.sec_id, t.semester, t.year from instructor as i, teaches as t \n\t"
                  "where i.ID = t.ID) as inst, (select c.building, c.room_number, s.course_id, s.sec_id, s.semester, s.year \n\t"
                  "from section as s, classroom as c where s.building = c.building and s.room_number = c.room_number) as class\n\t"
                  "where inst.course_id=class.course_id and inst.sec_id=class.sec_id and inst.semester=class.semester \n\t"
                  "and inst.year=class.year and class.building='Taylor';")

        elif user_input == 'i':
            migrator.init_tables_to_collection()

        elif user_input == 'c':
            migrator.print_mysql_mode = not migrator.print_mysql_mode

            print("Change Print Mode ", end="")
            print("MYSQL" if not migrator.print_mysql_mode else "MongoDB", end="")
            print(" to ", end="")
            print("MYSQL!" if migrator.print_mysql_mode else "MongoDB!")

        else:
            print("You Pressed Wrong Input!")
