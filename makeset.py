#!/bin/python
# This is the makesplit.py script. This breaks a data set into partitions for
# k-fold cross validation. By default, this script uses stratified partition
# scheme. This script can be simply and easily on any number of files by
# simply by calling python with it as an argument, and the files to be split
# as additional arguments, like so:
#
#  python makesplit.py file1 [ file2 ... filen ]
#
import sys, os, random

# This function splits a dictionary of feature lists into
# two different dictionaries, a partition with some partition%
# of each key's associated list, and a remainder dictionary, with
# 1-partition% of each key's associated list contained within.
# This function should only be run on data that has already been
# shuffled
def part_data(data, partition):
	partition_data = {}
	remainder_data = {}
	for each in data:
		partition_point = int(len(data[each])*partition)
		partition_data[each] = data[each][:partition_point]
		remainder_data[each] = data[each][partition_point:]
	return [partition_data, remainder_data]


# This breaks up a dictionary into some number of folds,
# This assumes that the data has been shuffled already
def shuffle_data(data):
	for each in data:
		random.shuffle(data[each])
	return data

def split_data(data, folds, chunk):
	fold = {}
	fold_length = {}
	for each in data:
		fold_length[each] = len(data[each])/folds

	if chunk is True:
		for i in range(folds):
			fold[i+1] = []
			current_fold = {}
			for each in sorted(data, key=int):
				current_fold[each] = []
				for j in range(fold_length[each]):
					current_fold[each].append(data[each].pop())
			fold[i+1] = dict(current_fold)
		for i in fold:
			for each in data:
				if len(data[each]) != 0:
					fold[i][each].append(data[each].pop())
		return fold
	for i in range(folds):
		fold[i+1] = []
		current_fold = {}
		for each in sorted(data, key=int):
			current_fold[each] = []
			for j in range(fold_length[each]):
				current_fold[each].append(data[each].pop())
				fold[i+1] = dict(current_fold)
	for i in fold:
		for each in data:
			if len(data[each]) != 0:
				fold[i][each].append(data[each].pop())
				return fold

# Determines necessary number of leading zeroes
def get_zeros(number):
	c = 10
	x = 1
	while True:
		if number / c**x == 0:
			break
		x+= 1
	x = 2 if x == 1 else x
	return x

state = None
params = {}
# Default options
# Portion of data to be set aside for parameter tuning
params['p'] = .266666
# Number of folds to use for cross validation
params['k'] = 10
# Number of folds to use for parameter tuning
params['j'] = 5
# Current output directory
params['o'] = 'output'
#Chunk splitting, false by default
params['b'] = False

# Parse out the arguments 
args = sys.argv
for each in args[1:]:      # for each arg
	if each.startswith('-'): # capture the flags
		k = args.pop(args.index(each)) # pop each arg

		# Parse command line options and record values
		if k[1] is 'p':
			if float(k[2:]) >= 1:
				print "Bad partition-value"
				quit()
			params[k[1]]=float(k[2:])
		elif k[1] is 'b':
			print "using chunk mode"
			params[k[1]] = True

		elif k[1] is 'k':
			if int(k[2:]) <= 1:
				print "Bad k-fold value, this hurts me physically"
				quit()
			params[k[1]]=int(k[2:])

		elif k[1] is 'j':
			if int(k[2:]) <= 1:
				print "Bad j-fold value, this hurts me physically"
				quit()
			params[k[1]]=int(k[2:])

		elif k[1] is 'o':
			if k[2] is not '=':
				print "Args are formatted wrong"
				quit()
			params[k[1]]=k[3:-1] if k[3:].endswith('/') else k[3:] 

# Iterating over files in path list
data = {}
if len(args[1:]) < 1:
	print "Partitioned nothing successfully"
	quit()
for file_name in args[1:]:
	basename = ''
	if state is not None:
		random.setstate(state)
	if not os.path.isfile(file_name):
		print file_name + ' was not found. Could not add data to batch'
		continue
	print "Attempting to parse data from " + file_name

	# Parsing file
	for each in open(file_name, "r").readlines():
		key = str(each).split()[0]
		if key not in data:
			data[key] = []
		if key in data:
			a = [float(x) for x in each.split()[1:-1]]
			a.append(each.split()[-1])
			data[key].append(a)


# Creating output directory 
try:
	basename = params['o'] 
	os.mkdir(basename)
except OSError:
	print "failed to create output directory: " + params['o']
	quit()


datlist = part_data(data, params['p'])
if not params['b']:
	datlist[0] = shuffle_data(datlist[0])
	datlist[1] = shuffle_data(datlist[1])
tuning = split_data(datlist[0], params['j'], params['b'])
evaluation = split_data(datlist[1], params['k'], params['b'])

# Writing tuning folds
x = get_zeros(params['j'])
for each in sorted(tuning, key=int):
	with open(basename + '/' + params['o'] + "_tuning_fold_" + \
			str(int(each)).zfill(x)+'.txt', 'a') as f:
		nu = '\n'
		for a in sorted(tuning[each], key=int):
			for j in tuning[each][a]:
				f.write(a + "\t" + "\t".join(map(str, j)) + nu)

# Writing validation folds
x = get_zeros(params['k'])
for each in sorted(evaluation, key=int):
	with open(basename + '/' + params['o'] + "_evaluation_fold_" + \
			str(int(each)).zfill(x)+'.txt', 'a') as f:
		nu = '\n'
		for a in sorted(evaluation[each], key=int):
			for j in evaluation[each][a]:
				f.write(a + "\t" + "\t".join(map(str, j)) + nu)

print "Successfully partitioned batch" 

