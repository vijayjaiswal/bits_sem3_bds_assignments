from PyInquirer import prompt
from examples import custom_style_2
from prompt_toolkit.validation import Validator, ValidationError

from pymongo import MongoClient
from bson.son import SON

import numpy as np

client = MongoClient()
db = client.rest
rest = db.rest


class NumberValidator(Validator):

    def validate(self, document):
        try:
            int(document.text)
        except ValueError:
            raise ValidationError(message="Please enter a number",
                                  cursor_position=len(document.text))




questions = [
    {
        'type': 'list',
        'name': 'user_option',
        'message': 'Welcome to MongoDB Data Query',
        'choices': ["Q1","Q2","Q3", "Q4"]
    }

    


]

def q1():
    print("\nQuestion 1:")
    print("User provides a choice of cuisine and zipcode, the query returns a list of \ntop 5 restaurant names with full address and rating with results sorted descending order based on rating.")
    print("\nEnter Zip code (example : CF243JH)")
    zc = input()
    print("Enter Type of food (example : Lebanese)")
    tf = input()

    # Sample Input - "CF243JH", "Lebanese"

    myquery = { "zipcode": zc, "type_of_food": tf}

    mydoc = rest.find(myquery).sort("rating", -1).limit(5)
    print("\nResult: ")
    for x in mydoc:
        print(x)

def q2():
    print("\nQuestion 2:")
    print("User provides a string to be searched in the address field and a minimum rating. \nThe query returns all restaurant names and cuisine that match the inputs provided along with the match score. \nSort output by descending score.")
    print("\nEnter Address (example : City)")
    ad = input()
    print("Enter minimum rating (example : 4)")
    mr = int(input())

    #Sample Input City, 4

    myquery = {"address": { "$regex": ad}, "rating": { "$gt": mr} }
    fields = {'name': 1, 'type_of_food': 1}

    # TODO: How to identify match score
    mydoc = rest.find(myquery, fields).sort("rating", -1)
    print("\nResult: ")
    for x in mydoc:
        print(x)

def q3() :
    print("\nQuestion 3:")
    print("Give a cuisine, output how many (i.e. count) matching restaurants are there per zip code. \nSort descending the 2 column output (zipcode, count) by the count.")
    print("\nEnter Type of food (example: Lebanese)")
    tf = input()
    myquery = {"type_of_food": tf}
    mydoc = rest.aggregate([
        {"$match":myquery},
        {"$group":{"_id":{"ZIPCODE":"$zipcode"},
                "Count":{"$sum":1}}},
        {"$sort":SON([("Count", -1)])}
    ]);
    print("\nResult: ")
    for x in mydoc:
        print(x)

def q4() :
    print("\nQuestion 4:")
    print("Show the average rating per type of food and provide an ascending sorted output \n(type of food, rating) by rating.")
    mydoc = rest.aggregate([
        {"$group":{"_id":{"Type Of Food":"$type_of_food"},
                "AVG_RATING":{"$avg":"$rating"}}},
        {"$sort":SON([("AVG_RATING", -1)])}
    ]);
    print("\nResult: ")
    for x in mydoc:
        print(x)

def add(a, b):
    print(a + b)

def difference(a, b):
    print(a - b)

def product(a, b):
    print(a * b)

def divide(a, b):
    print(a / b)


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