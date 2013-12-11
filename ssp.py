#!/usr/bin/env python

import sqlite3
import fileinput
import sys
import cStringIO
import re

from optparse import OptionParser

def enum(**enums):
    return type('Enum', (), enums)

class SSPConfig(object):
	def __init__(self, config):
		self.config = config

	def get(self, config_value, default = None):
		val = self.config.get(config_value)
		if val == None:
			return default
		
		return val

	def set(self, config, value):
		self.config[config] = value

class SSP(object):
	
	DB_LOCATION = ":memory:"
	DEFAULT_TABLE_NAME = 'ssp_data'

	COLUMN_PROVIDERS = enum( 
		FIRST_LINE = "FIRST_LINE", 
		STATIC = "STATIC", 
		DYNAMIC = "DYNAMIC"
	)

	def __init__(self, **config):
		self.config = SSPConfig(config)
		self.conn = sqlite3.connect(self.config.get('db_file'))
		self.cursor = self.conn.cursor()
		self.columns = []

	def define_columns(self, columns):
		self.columns = columns
		
		self.cursor.execute('DROP TABLE IF EXISTS {0}'.format(self.getTableName()))

		create_table_statement = """CREATE TABLE {table_name} 
			( {column_statements} )""".format(
			table_name = self.getTableName(),
			column_statements = ',\n'.join(['\t{0} {1}'.format(k,v) for k,v in columns])
			)
 
		self.cursor.execute(create_table_statement)
		
		self.conn.commit()

	def getTableName(self):
		return self.config.get('table_name', SSP.DEFAULT_TABLE_NAME)

	def insert_data(self, data):
	
		insert_statement = """INSERT INTO {table_name} 
			( {column_line} ) 
			VALUES ( {columns} )""".format(
				table_name = self.getTableName(),
				column_line = ','.join([desc for desc,_ in self.columns]),
				columns = ','.join(['?'] * len(self.columns))
				)

		self.cursor.execute(insert_statement, tuple(data))

	def onLineError(self, text):
		if self.config.get('ignore_wrong_lines', default = True):
			sys.stderr.write(text)
			sys.stderr.write('\n')
		else:
			raise Exception(text)

	def grow_columns(self,new_length):
		columns_len = len(self.columns)

		if columns_len == 0:
			column_defs = [('column{0}'.format(r),'UNKNOWN') for r in range(columns_len, new_length)]
			self.define_columns(column_defs)
			return

		for i in range(columns_len, new_length):
			alter_table_statement = 'ALTER TABLE {table_name} ADD COLUMN column{column} UNKNOWN'.format( table_name = self.getTableName(), column=i )
			
			self.cursor.execute(alter_table_statement)
			self.conn.commit()

			self.columns.append( ('column{0}'.format(i), 'unknown') )

	def process(self, files):
		
		column_provider = self.config.get('column_provider', default = SSP.COLUMN_PROVIDERS.DYNAMIC)
		firstlinecolumns = False

		if column_provider == SSP.COLUMN_PROVIDERS.FIRST_LINE:
			firstlinecolumns = True

		if column_provider == SSP.COLUMN_PROVIDERS.STATIC:
			column_defs = [c.split(':') for c in self.config.get('columns').split(',')]
			self.define_columns(column_defs)

	
		delimiter = self.config.get('delimiter', default = ' ')

		line_counter = 0

		skip_lines = self.config.get('skip_lines', default = 0)

		for line in fileinput.input(files):
			line_counter += 1

			if skip_lines > 0:
				skip_lines -= 1
				continue

			line = line.strip()
			split_line = re.split(delimiter, line)
			
			if self.columns == None and firstlinecolumns:
				firstlinecolumns = False
				column_defs = [(k,'UNKNOWN') for k in split_line]
				self.define_columns(column_defs)
				continue

			split_line_len = len(split_line)
			columns_len = len(self.columns)

			data = split_line

			insert = True

			if split_line_len > columns_len:
				if column_provider == SSP.COLUMN_PROVIDERS.DYNAMIC:
					self.grow_columns(split_line_len)

				elif self.config.get('join_long_rows', default = True):
					last_data = ' '.join(data[(columns_len-1):])
					data[(columns_len-1):] = []
					data.append(last_data)

				else:
					self.onLineError('Line {0} too long ({1}/{2})'.format(line_counter, split_line_len, columns_len))
					insert = False



			if split_line_len < columns_len:

				if self.config.get('fill_short_rows', default = False) or column_provider == SSP.COLUMN_PROVIDERS.DYNAMIC: 
					data[split_line_len:] = ([None] * (columns_len - split_line_len))
				else:
					self.onLineError('Line {0} too short ({1}/{2})'.format(line_counter, split_line_len, columns_len))
					insert = False


			if insert:
				self.insert_data(data)


		self.conn.commit()

	def dumpDB(self):
		print ','.join([desc for desc,_ in self.columns])
		self.cursor.execute('SELECT * FROM {0}'.format(self.getTableName()))
		for row in self.cursor:
			for field in row:
				print field,
			print ''

	def execute(self, command):
		self.cursor.execute(command)
		delimiter = self.config.get('output_delimiter', default=' ')
		for row in self.cursor:
			print delimiter.join([str(r) for r in row])

	def close(self):
		self.conn.commit()
		self.cursor.close()
		self.conn.close()

def main():
	parser = OptionParser()
	parser.add_option('-t', '--table', dest = 'table_name', metavar='TABLE_NAME', default=SSP.DEFAULT_TABLE_NAME)
	parser.add_option('-s', '--skip', dest = 'skip_lines', metavar='LINES', default=0, type="int")
	parser.add_option('-i', '--ignore-wrong-lines', dest = 'ignore_wrong_lines',  default=True, action='store_false')
	parser.add_option('-1', '--first-line-headers', dest = 'column_provider', action='store_const', const = SSP.COLUMN_PROVIDERS.FIRST_LINE)
	parser.add_option('-c', '--columns', dest = 'columns', metavar='COLUMNS')
	parser.add_option('--dynamic-columns', dest = 'column_provider', action='store_const', const = SSP.COLUMN_PROVIDERS.DYNAMIC)
	parser.add_option('-j', '--join-long-rows', dest = 'join_long_rows', default=False, action='store_true')
	parser.add_option('-f', '--fill-short-rows', dest = 'fill_short_rows', default=False, action='store_true')
	parser.add_option('-d', '--delimiter', dest = 'delimiter', default=' ')
	parser.add_option('-o', '--output-delimiter', dest = 'output_delimiter', default=' ')
	parser.add_option('--db', dest = 'db_file', default=SSP.DB_LOCATION)

	(options, args) = parser.parse_args()

	options_dict = vars(options)

	if options_dict['columns'] != None:
		options_dict["column_provider"] = SSP.COLUMN_PROVIDERS.STATIC
	
	if options_dict['column_provider'] == SSP.COLUMN_PROVIDERS.DYNAMIC:
		options_dict['fill_short_rows'] = True

	ssp = SSP( **options_dict )

	ssp.process(args[1:])

	ssp.execute(args[0])

	ssp.close()


if __name__ == '__main__':
	main()
