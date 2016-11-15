# Makefile for DBMS Project 2
# CS 04-530, Summer 2014, Dr. John Robinson, Rowan University

SRCPATH = src
BINPATH = bin
SOLJARS = lib/bufmgr.jar

JAVAC = javac -d $(BINPATH) -sourcepath $(SRCPATH)\;$(SOLJARS)
JAVA  = java -classpath $(BINPATH)\;$(SOLJARS)



all: global diskmgr bufmgr heap tests

global:
	$(JAVAC) $(SRCPATH)/global/*.java

diskmgr:
	$(JAVAC) $(SRCPATH)/diskmgr/*.java

bufmgr:
	$(JAVAC) $(SRCPATH)/bufmgr/*.java
	
heap:
	$(JAVAC) $(SRCPATH)/heap/*.java

tests:
	$(JAVAC) $(SRCPATH)/tests/*.java

clean:
	\find . -name \*.class -exec rm -f {} \;