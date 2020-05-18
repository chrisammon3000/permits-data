#!/bin/bash

read -p 'Delete current database? [y/n]: ' response
case "$$response" in 
    [yY][eE][sS]|[yY])
        echo "### Deleting PostgreSQL Database... ###"
        echo 'Removing files in ./postgres/pgdata/ ...'
        #@echo "Enter password to continue:"
        sudo rm -rf ./postgres/pgdata/
        echo "Done."
    ;;
    *) 
        echo "Aborting..."
        exit
    ;;
esac