#!/bin/bash

APP=tap_rest_api

PYTHON=/opt/python/3.6/bin/python
if [ ! -e $PYTHON ]; then
    PYTHON=`which python3`
fi
echo $PYTHON

if [ -e ./install_test ]; then
    rm -fr install_test
fi

$PYTHON -m venv install_test
source install_test/bin/activate;
find $APP -name '__pycache__' | xargs rm -fr;
python setup.py clean --all;
rm -fr dist;
rm -fr build;
rm -fr $APP.egg-info;
python setup.py install;

SITE_PKG_DIR="./install_test/lib/python3.6/site-packages"
PKG_DIR=`ls $SITE_PKG_DIR | grep $APP`

# tree $SITE_PKG_DIR/$PKG_DIR/$APP
DIFF=`diff --exclude=__pycache__ -r $SITE_PKG_DIR/$PKG_DIR/$APP ./$APP`
if [ -z "$DIFF" ]
then 
    echo "All file are included in the package.";
else
    echo $DIFF
    echo "Check MANIFEST.in"
    exit 1;
fi

# Note: Don't insert spaces in the next line
$APP&>install_test/msg
CMD_OUT=`cat install_test/msg | grep "usage: $APP"`
if [ -z "$CMD_OUT"  ]; then
    cat install_test/msg
    echo "$APP is not properly installed"
    exit 1;
else
    echo "$APP command is returning the expected message."
fi

deactivate
echo "Install test finished successfully"
