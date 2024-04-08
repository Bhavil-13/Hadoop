#!/bin/bash

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_from_1_to_7>"
    exit 1
fi

# Assigning the provided argument to a variable
number=$1

# Associating actions with numbers
case $number in
    [1-6])
        # Associating names with numbers 1 to 6
        case $number in
            1)
                name="TopWords.java"
                ;;
            2)
                name="CoOccurrenceMatrix.java"
                ;;
            3)
                name="CoOccurrenceStripe.java"
                ;;
            4)
                name="AggCoOccurrenceStripe.java"
                ;;
            5)
                name="DocumentFrequency.java"
                ;;
            6)
                name="TFIDFStripes.java"
                ;;
        esac
        
        # Copying the specified file to the source directory
        cp "/home/bhavil/Desktop/NoSQL-A2/demo/codes/$name" "/home/bhavil/Desktop/NoSQL-A2/demo/src/main/java/sample"


        mvn clean install
        ;;
    7)
        # Removing the current file inside /home/bhavil/Desktop/NoSQL-A2/demo/src/main/java/sample
        sudo rm -f "/home/bhavil/Desktop/NoSQL-A2/demo/src/main/java/sample/*"
        ;;
    *)
        echo "Invalid number. Please provide a number from 1 to 7."
        exit 1
        ;;
esac

# Running maven clean install
