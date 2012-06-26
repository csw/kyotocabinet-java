# Makefile for sample programs of Kyoto Cabinet



#================================================================
# Setting Variables
#================================================================


# Generic settings
SHELL = /bin/sh

# Targets
MYCLASSES = KCDBEX1.class KCDBEX2.class

# Building binaries
JAVAC = javac
JAVACFLAGS = -cp .:../kyotocabinet.jar
JAVARUN = java
JAVARUNFLAGS = -cp .:../kyotocabinet.jar -Djava.library.path=.:..:/usr/local/lib



#================================================================
# Suffix rules
#================================================================


.SUFFIXES :
.SUFFIXES : .java .class

.java.class :
	$(JAVAC) $(JAVACFLAGS) $<



#================================================================
# Actions
#================================================================


all : $(MYCLASSES)


run :
	rm -rf casket*
	$(JAVARUN) $(JAVARUNFLAGS) KCDBEX1
	$(JAVARUN) $(JAVARUNFLAGS) KCDBEX2


clean :
	rm -rf $(MYCLASSES) *.class casket* *~


.PHONY : all clean



# END OF FILE
