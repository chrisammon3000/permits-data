#!/bin/bash

# Trying to add confirmation
read -p 'Delete current database? [y/n]: ' response
case "$$response" in 
    [yY][eE][sS]|[yY])
        echo "### Deleting PostgreSQL Database... ###"
        echo 'Removing files in ./postgres/pgdata/ ...'
        #@echo "Enter password to continue:"
        sudo rm -rf "$PWD/postgres/pgdata/*"
        echo "Done."
        break
    ;;
    *) 
        echo "Aborting..."
        exit
    ;;
esac