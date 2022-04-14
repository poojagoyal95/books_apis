import mysql.connector
from dotenv import load_dotenv
import os
load_dotenv()
from flask import Flask, request,jsonify
from flask_mysqldb import MySQL
import json

app = Flask(__name__)
app.config["MYSQL_HOST"] = os.getenv("HOST")
app.config["MYSQL_USER"] = os.getenv("DB_USERNAME")
app.config["MYSQL_PASSWORD"] = os.getenv("DB_PASSWORD")
app.config["MYSQL_DB"] = os.getenv("DB_DATABASE")
mysql = MySQL(app)

@app.route("/",methods=['GET'])
def query1():
    args = request.args
    page = args.get("page", default=0, type=int)
    cursor = mysql.connection.cursor()
    itemsPerPage = 25
    offset = (page - 1) * itemsPerPage + 1
    result = cursor.execute("""SELECT book.id,book.title,author.name,author.birth_year,author.death_year,GROUP_CONCAT(shelf.name) as book_shelves,languages.code,GROUP_CONCAT(subjects.name) as subjects,format.mime_type,format.url FROM books_book book LEFT JOIN books_book_authors book_author ON book_author.book_id = book.id LEFT JOIN books_author author ON author.id = book_author.author_id LEFT JOIN books_book_bookshelves book_shelv on book_shelv.book_id = book.id LEFT JOIN books_bookshelf shelf on shelf.id = book_shelv.bookshelf_id LEFT JOIN books_book_languages book_lang on book_lang.book_id = book.id LEFT JOIN books_language languages on languages.id = book_lang.language_id LEFT JOIN books_book_subjects book_sub on book_sub.book_id = book.id LEFT JOIN books_subject subjects on subjects.id = book_sub.subject_id LEFT JOIN books_format format on book.id = format.book_id group by book.id order by book.download_count desc LIMIT %s,%s""",(page,itemsPerPage))
    fetchedData = cursor.fetchall()
    response = app.response_class(
        response=json.dumps(fetchedData),
        status=200,
        mimetype='application/json'
    )
    return response



@app.route("/query",methods=['GET'])
def query2():
    args = request.args
    cursor = mysql.connection.cursor()
    query = "SELECT book.id,book.title,book.gutenberg_id,format.mime_type FROM `books_book` as book LEFT JOIN books_format as format on book.id =format.book_id LEFT JOIN books_book_languages book_lang on book_lang.book_id = book.id"
    if args.get("language") and not args.get("topic"):
        query += " LEFT JOIN books_language languages on languages.id = book_lang.language_id where languages.code like '%{}%'".format(args.get("language"))

    if args.get("topic") and not args.get("language"):
        query += " LEFT JOIN books_book_bookshelves book_shelv on book_shelv.book_id = book.id LEFT JOIN books_bookshelf shelf on shelf.id = book_shelv.bookshelf_id LEFT JOIN books_book_subjects book_sub on book_sub.book_id = book.id LEFT JOIN books_subject subjects on subjects.id = book_sub.subject_id where shelf.name like '%{}%' or subjects.name like '{}%'".format(args.get("topic"),args.get("topic"))

    if args.get("language") and args.get("topic"):
        query += " LEFT JOIN books_book_bookshelves book_shelv on book_shelv.book_id = book.id LEFT JOIN books_bookshelf shelf on shelf.id = book_shelv.bookshelf_id LEFT JOIN books_book_subjects book_sub on book_sub.book_id = book.id LEFT JOIN books_subject subjects on subjects.id = book_sub.subject_id LEFT JOIN books_language languages on languages.id = book_lang.language_id where languages.code like '%{}%' or shelf.name like '%{}%' or subjects.name like '{}%'".format(args.get("language"),args.get("topic"),args.get("topic"))

    result = cursor.execute(query)
    fetchedData = cursor.fetchall()
    response = app.response_class(
        response=json.dumps(fetchedData),
        status=200,
        mimetype='application/json'
    )
    return response

if __name__ == '__main__':
    app.run(debug=True)



  
