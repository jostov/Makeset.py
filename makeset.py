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

# This disorders the data dictionary
def shuffle_data(data):
  for each in data:
    random.shuffle(data[each])
  return data

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
def split_data(data, folds):
  fold = {}
  fold_length = {}
  for each in data:
    fold_length[each] = len(data[each])/folds
  for i in range(folds):
    fold[i+1] = []
    current_fold = {}
    for each in sorted(data, key=int):
      floor = 0 if i == 0 else i * fold_length[each] 
      ceiling = (i + 1) * fold_length[each]
      current_fold[each] = data[each][floor:ceiling]
    fold[i+1] = dict(current_fold)
  return fold

# Determines necessary number of leading zeroes
def get_zeros(number):
  c = 10
  x = 1
  while True:
    if number / c**x == 0:
      break
    x+= 1
  return x

state = None
params = {}
# Portion of data to be set aside for parameter tuning
params['p'] = .266666
# Number of folds to use for cross validation
params['k'] = 10
# Number of folds to use for parameter tuning
params['j'] = 5
# Current output directory
params['o'] = '.'

# Iterating over files in path list
paths = sys.argv[1:]
for file_name in paths:
  basename = ''
  if state is not None:
    random.setstate(state)
  if not os.path.isfile(file_name):
    print file_name + ' was not found.'
    continue
  print "Attempting to partition data from " + file_name

  # Creating output directory 
  try:
    basename = params['o'] + '/' + os.path.split(file_name)[-1][:-4]
    os.mkdir(basename)
  except OSError:
    print "failed to create output directory for " + file_name
    continue

  # Parsing file
  data = {}
  for each in open(file_name, "r").readlines():
    key = str(each).split()[0]
    if key not in data:
      data[key] = []
    if key in data:
      a = [float(x) for x in each.split()[1:-1]]
      a.append(each.split()[-1])
      data[key].append(a)
  datlist = part_data(shuffle_data(data), .266666)
  tuning = split_data(datlist[0], params['j'])
  validation = split_data(datlist[1], params['k'])

  # Writing tuning folds
  x = get_zeros(params['j'])
  for each in sorted(tuning, key=int):
    with open(basename + '/' + file_name[:-4] + "_tuning_fold_" + \
        str(int(each)).zfill(x), 'a') as f:
      nu = '\n'
      for a in sorted(tuning[each], key=int):
        for j in tuning[each][a]:
          f.write(a + "\t" + "\t".join(map(str, j)) + nu)

  # Writing validation folds
  x = get_zeros(params['k'])
  for each in sorted(validation, key=int):
    with open(basename + '/' + file_name[:-4] + "_validation_fold_" + \
        str(int(each)).zfill(x), 'a') as f:
      nu = '\n'
      for a in sorted(validation[each], key=int):
        for j in validation[each][a]:
          f.write(a + "\t" + "\t".join(map(str, j)) + nu)

  print "Successfully partitioned " + str(file_name)

