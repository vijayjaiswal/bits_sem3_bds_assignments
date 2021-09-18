# Part 2: Data query application

# Write an end user facing query application which can run the following query types after taking required user inputs.
# Only specified output fields need to be returned.
# 1.	User provides a choice of cuisine and zipcode, the query returns a list of top 5 restaurant names with full
#       address and rating with results sorted descending order based on rating.
# 2.	User provides a string to be searched in the address field and a minimum rating. The query returns all
#       restaurant names and cuisine that match the inputs provided along with the match score.
#       Sort output by descending score.
# 3.	Give a cuisine, output how many (i.e. count) matching restaurants are there per zip code.
#       Sort descending the 2 column output (zipcode, count) by the count.
# 4.	Show the average rating per type of food and provide an ascending sorted output (type of food, rating) by rating.
#
# You can create the data query application with the 4 types of query types encoded as functions.
# The user can run this program from the command line with parameters : type of query (1-4) and corresponding inputs.
# The query is executed on the DB and results returned on console.

from prompt_toolkit.validation import Validator, ValidationError

from pymongo import MongoClient
from bson.son import SON

# Creating Mongodb connectivity to "rest" database
client = MongoClient('localhost', 27017)
# Database Name : rest
db = client.rest
# Collection Name : rest
rest = db.rest

# Generic class for Number Validation
class NumberValidator(Validator):
    # Function to validate text is a number
    def validate(self, document):
        try:
            int(document.text)
        except ValueError:
            raise ValidationError(message="Please enter a number",
                                  cursor_position=len(document.text))

# Banner for User Input
questions = [
    {
        'type': 'list',
        'name': 'user_option',
        'message': 'Welcome to MongoDB Data Query',
        'choices': ["Q1","Q2","Q3", "Q4"]
    }
]

# Generic method to display unique values of an attribute of a collection
def get_unique_values(collection, attribute):
    attributes = "Possible "+attribute+":"
    for values in collection.distinct(attribute):
        attributes = attributes + "," + str(values)
    print(attributes)

# Generic method to execute Mongodb find query on a collection
def execute_query(collection, myquery,fields, sort_col, sort_col_order, limits):
    if fields!="":
        mydoc = collection.find(myquery, fields).sort(sort_col, sort_col_order).limit(limits)
    else:
        mydoc = collection.find(myquery).sort(sort_col, sort_col_order).limit(limits)

    print("Executing: ",myquery,"\nQuery Output: ")
    count=collection.count_documents(myquery)
    if count!= 0:
        print("Found", count, "documents matching the query :) (displaying only ", limits, ")")
        for x in mydoc:
            print(x)
    else:
        print("No record found for query! :( ")

# Generic method to execute Mongodb aggregate query on a collection
def execute_query_aggregate(collection, myquery, groupBy, sort_col, sort_col_order, limits):
    agg_query=[
        {"$match": myquery},
        {"$group": groupBy},
        {"$sort": {sort_col: sort_col_order}},
        {"$limit": limits}
    ]
    mydoc = rest.aggregate(agg_query);

    print("Executing: ",agg_query,"\nQuery Output: ")
    count=collection.count_documents(myquery)
    if count!=0:
        print("Found",count,"documents matching the query :) (displaying only ",limits,")")
        for x in mydoc:
            print(x)
    else:
        print("No record found for query! :( ")

# Method to execute Q1
def execute_q1(zipcode, type_of_food):
    # Sample Input - "CF243JH", "Lebanese"
    myquery = {"zipcode": zipcode, "type_of_food": type_of_food}
    fields = ""
    execute_query(rest, myquery, fields, "rating", -1, 10)

# Method to execute Q2
def execute_q2(address, rating):
    myquery = {"address": { "$regex": address}, "rating": { "$gt": rating} }
    fields = {'name': 1, 'type_of_food': 1, 'address':1, 'rating':1 }
    execute_query(rest, myquery, fields, "rating", -1, 10)

# Method to execute Q3
def execute_q3(type_of_food):
    myquery = {"type_of_food": type_of_food}
    groupBy = {"_id": {"ZIPCODE": "$zipcode", "type_of_food": "$type_of_food"}, "Count": {"$sum": 1}}
    execute_query_aggregate(rest, myquery, groupBy, "Count", -1, 10)

# Method to execute Q4
def execute_q4():
    myquery = {}
    groupBy = {"_id": {"Type Of Food": "$type_of_food"}, "AVG_RATING": {"$avg": "$rating"}}
    execute_query_aggregate(rest, myquery, groupBy, "AVG_RATING", -1, 10)

# Method for Question 1
def q1():
    print("\nQuestion 1:")
    print("User provides a choice of cuisine and zipcode, the query returns a list of \ntop 5 restaurant names with full address and rating with results sorted descending order based on rating.")
    print("\nEnter Zip code (example : CF243JH)")
    get_unique_values(rest, 'zipcode')

    zc = input()
    print("Enter Type of food (example : Lebanese)")
    get_unique_values(rest, 'type_of_food')
    tf = input()

    # Sample Input - "CF243JH", "Lebanese"
    execute_q1(zc,tf)


# Method for Question 2
def q2():
    print("\nQuestion 2:")
    print("User provides a string to be searched in the address field and a minimum rating. \nThe query returns all restaurant names and cuisine that match the inputs provided along with the match score. \nSort output by descending score.")
    print("\nEnter Address (example : City)")
    get_unique_values(rest, 'address')
    ad = input()
    print("Enter minimum rating (example : 4)")
    get_unique_values(rest, 'rating')
    mr = int(input())

    #Sample Input City, 4
    execute_q2(ad, mr)

# Method for Question 3
def q3() :
    print("\nQuestion 3:")
    print("Give a cuisine, output how many (i.e. count) matching restaurants are there per zip code. \nSort descending the 2 column output (zipcode, count) by the count.")
    print("\nEnter Type of food (example: Lebanese)")
    get_unique_values(rest, 'type_of_food')
    tf = input()

    execute_q3(tf)

# Method for Question 4
def q4() :
    print("\nQuestion 4:")
    print("Show the average rating per type of food and provide an ascending sorted output \n(type of food, rating) by rating.")
    execute_q4()


# Main Method to Run the application and get user's input
def main():
    print("##############################################################################")
    print("Welcome to MongoDB Data Query")
    print("##############################################################################")
    while (True) :
        print("\n\nPlease Select Question number (valid options: Q1, Q2, Q3, Q4, Q(to quit)) :")
        answers = input()
        if answers.lower() == "q1":
            q1()
        elif answers.lower() == "q2":
            q2()
        elif answers.lower() == "q3":
            q3()
        elif answers.lower() == "q4":
            q4()
        elif answers.lower() == "q":
            break
        else:
            print("Expected options are (Q1, Q2, Q3, Q4, Q)")

if __name__ == "__main__":
    main()