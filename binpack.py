#############################################################################
# Author: Chelsea Marie Hicks
# OSU Email: hicksche@oregonstate.edu
# Course number/section: CS 325-401
# Assignment: Homework 8            Due Date: May 31, 2020 by 11:59 PM
#
# Description: Program reads inputs from a file named bin.txt which
#       consists of test cases where the first line is the number of 
#       test cases, followed by the capacity of bins for that test case
#       the number of items, and then the weight of each item.
#       
#       With this information, the program runs three separate algorithms
#       aimed at optimizing the bin packing problem, which is to pack items
#       of different weights into the fewest number of bins with a specific
#       capacity. The three algorithms used to solve the bin packing problem
#       in this program are First-Fit, First-Fit-Decreasing, and Best Fit.
#       The specifics of each algorithm are described in comments before their
#       declaration. In the end, each algorithm will return the number of bins
#       found to be needed according to that algorithm.
#############################################################################

#firstFit puts each item encountered into the earliest open bin that it can
#fit into. If there isn't a bin with enough space, then a new bin is opened.
def firstFit(capacity, items, weights):
    #setup all variables for loops
    binCount = 0
    bin = [capacity] * items
    binCap = capacity
    numItems = items
    itemWeights = weights

    #for loop goes through each item to place the item into a bin
    for item in range(numItems):
        index = 0
        #while loop finds the bin to place the item into based on space
        #and what's open
        while(index < binCount):
            if(bin[index] >= itemWeights[item]):
                bin[index] = bin[index] - itemWeights[item]
                break
            index += 1
        #If there wasn't a bin for the item to go into, open a new one
        if(index == binCount):
            bin[binCount] = binCap - itemWeights[item]
            binCount += 1

    return binCount

#firstFitDec first sorts the items indecreasing order and then calls firstFit
#on that ordered list
def firstFitDec(capacity, items, weights):
    sortedWeights = sorted(weights, reverse=True)
    return firstFit(capacity, items, sortedWeights)

def bestFit(capacity, items, weights):
    binCount = 0
    bin = [capacity] * items
    binCap = capacity
    numItems = items
    itemWeights = weights

    #for loop goes through each item to place the item into a bin
    for item in range(numItems):
        index = 0
        min = binCap
        bestBin = 0

        #places the item in the bin that will have the least room left over with that
        #item in the bin and if it cannot fit, create a new bin.
        while(index < binCount):
            if(bin[index] >= itemWeights[item] and bin[index] - itemWeights[item] < min):
                bestBin = index
                min = bin[index] - itemWeights[item]
            index += 1
        #if item wasn't placed into a bin, create a new bin
        if(min == binCap):
            bin[binCount] = binCap - itemWeights[item]
            binCount += 1
        else:
            bin[bestBin] -= itemWeights[item]
    
    return binCount



#main operations of the program
def main():
    #open file with input data and read in each line to set to variables
    with open("bin.txt", "r") as inputFile:
        testCase = int(inputFile.readline().rstrip())

        #Loop through each test case
        for entry in range(testCase):
            #set the capacity equal to the entry read in
            capacity = int(inputFile.readline().rstrip())

            #set the number of items equal to the entry read in
            items = int(inputFile.readline().rstrip())

            #create an array for the weights entered on the next line
            weights = list(map(int, inputFile.readline().rstrip().split()))

            binsFirstFit = firstFit(capacity, items, weights)
            binsDecFirstFit = firstFitDec(capacity, items, weights)
            binsBestFit = bestFit(capacity, items, weights)

            print("Test case %d" % (entry+1))
            print("First-Fit: %d" % (binsFirstFit))
            print("First-Fit-Decreasing: %d" % (binsDecFirstFit))
            print("Best Fit: %d" % (binsBestFit))
            print("\n")

if __name__ == "__main__":
    main()