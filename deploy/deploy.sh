#! /bin/bash

helm upgrade --install --create-namespace --atomic --timeout 2m30s --namespace dolosse production .
