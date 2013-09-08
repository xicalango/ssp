#!/usr/bin/env python

import sqlite3
import fileinput
import sys
import cStringIO
import re

from optparse import OptionParser

class SSP:
	
	DB_LOCATION = ":memory:"
	DEFAULT_TABLE_NAME = 'ssp_data'

	def __init__(self, **config):
		self.config = config
		self.conn = sqlite3.connect(self.getConfig('db_file'))
		self.cursor = self.conn.cursor()

	def define_columns(self, columns):
		self.columns = columns
		self.column_line = ','.join([desc for desc,_ in columns])
		
		self.cursor.execute('DROP TABLE IF EXISTS {0}'.format(self.getTableName()))

		output = cStringIO.StringIO()
		output.write('CREATE TABLE ')
		output.write(self.getTableName())
		output.write(' (\n')

		columnStatements = ',\n'.join(['\t{0} {1}'.format(k,v) for k,v in columns])	

		output.write(columnStatements)

		output.write(')')

		self.cursor.execute(output.getvalue())
		self.conn.commit()

	def getConfig(self, config_value, default = None):
		val = self.config.get(config_value)
		if val == None:
			return default
		
		return val

	def getTableName(self):
		return self.getConfig('table_name', SSP.DEFAULT_TABLE_NAME)

	def insert_data(self, data):
	
		#print self.column_line
		#print data

		output = cStringIO.StringIO()

		output.write('INSERT INTO ')
		output.write(self.getTableName())
		output.write('(')
		output.write( self.column_line )
		output.write(') VALUES (')
		output.write(','.join(['?'] * len(self.columns)))
		output.write(')')

		insert_statement = output.getvalue()

		self.cursor.execute(insert_statement, tuple(data))

	def onLineError(self, text):
		if self.getConfig('ignore_wrong_lines', default = True):
			sys.stderr.write(text)
			sys.stderr.write('\n')
		else:
			raise Exception(text)

	def process(self, files):
		
		firstlinecolumns = self.getConfig('first_line_headers', default = False)
		delimiter = self.getConfig('delimiter', default = ' ')

		line_counter = 0

		for line in fileinput.input(files):
			line_counter += 1

			line = line.strip()
			split_line = re.split(delimiter, line)
			
			if firstlinecolumns:
				firstlinecolumns = False
				column_defs = [(k,'UNKNOWN') for k in split_line]
				self.define_columns(column_defs)
				continue

			split_line_len = len(split_line)
			columns_len = len(self.columns)

			data = split_line

			insert = True

			if split_line_len > columns_len:
				if self.getConfig('join_long_rows', default = True):
					last_data = ' '.join(data[(columns_len-1):])
					data[(columns_len-1):] = []
					data.append(last_data)
				else:
					self.onLineError('Line {0} too long ({1}/{2})'.format(line_counter, split_line_len, columns_len))
					insert = False



			if split_line_len < columns_len:

				if self.getConfig('fill_short_rows', default = False):
					data[split_line_len:] = ([''] * (columns_len - split_line_len))
				else:
					self.onLineError('Line {0} too short ({1}/{2})'.format(line_counter, split_line_len, columns_len))
					insert = False


			if insert:
				self.insert_data(data)


		self.conn.commit()

	def dumpDB(self):
		print self.column_line
		self.cursor.execute('SELECT * FROM {0}'.format(self.getTableName()))
		for row in self.cursor:
			for field in row:
				print field,
			print ''

	def execute(self, command):
		self.cursor.execute(command)
		for row in self.cursor:
			print '|'.join([str(r) for r in row])

	def close(self):
		self.conn.commit()
		self.cursor.close()
		self.conn.close()


parser = OptionParser()
parser.add_option('-t', '--table', dest = 'table_name', metavar='TABLE_NAME', default=SSP.DEFAULT_TABLE_NAME)
parser.add_option('-i', '--ignore-wrong-lines', dest = 'ignore_wrong_lines',  default=True, action='store_false')
parser.add_option('-1', '--first-line-headers', dest = 'first_line_headers', default=True, action='store_false')
parser.add_option('-j', '--join-long-rows', dest = 'join_long_rows', default=True, action='store_false')
parser.add_option('-f', '--fill-short-rows', dest = 'fill_short_rows', default=False, action='store_true')
parser.add_option('-d', '--delimiter', dest = 'delimiter', default=' ')
parser.add_option('--db', dest = 'db_file', default=SSP.DB_LOCATION)

(options, args) = parser.parse_args()

options_dict = vars(options)

ssp = SSP( **options_dict )

ssp.process(args[1:])

ssp.execute(args[0])

ssp.close()
