#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import MySQLdb
# import requests

# use dcod.run() function to see it working properly
# it will return True if it worked
# also, you can see the test db I'm using 
# using phpMyAdmin at db4free.net
# use login: shallow
# pw: 123456

def parse_json(filepath):
	# data = requests.get(url).text
	with open(filepath, 'r') as f:
	    data = json.loads(f.read())
	structure = data['structure']
	result = []
	for item in data['data']:
		row = []
		for key in structure:
			row.append( item[key] )
		result.append( row )
	# sort it?
	# result = sorted(result)
	print('JSON parsed.')
	return [result, structure]


def generate_statement(data, structure, table = 'testtable'):
	values = "VALUES "
	for row in data:
		values = values + "( '%s', '%s', '%s')," % \
		( row[0], row[1], row[2] )
	values = values[:-1] + ";"
	statement = "INSERT INTO %s (%s, %s, %s) " % (table, structure[0], structure[1], structure[2] )
	print('SQL statement generated.')
	return statement + values


def db_interact(statement):
	try:
		conn = MySQLdb.connect('db4free.net', 
							'shallow', 
							'123456', 
							'dcodtest',
							use_unicode = True,
							charset = 'utf8')
		print("Connection to MySQL DB established.")
	except:
		print("Failed when trying to connect.")
		return False
	cursor = conn.cursor()
	try:
		cursor.execute( statement )
		conn.commit()
		print("SQL statement successfully executed.")
	except:
		conn.rollback()
		print ("Failed on execution/commit.")
		return False
	conn.close()
	return True


def run():
	json_parsed = parse_json('data.json')
	success = (db_interact( generate_statement( json_parsed[0] ,json_parsed[1] ) ))
	# print success