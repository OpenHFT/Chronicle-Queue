# Chronicle Queue User Guide

This folder contains the Chronicle Queue User Guide. It is arranged to suit the Antora build process. All pages must be located in a module under the corresponding pages/-folder, have the extension .adoc and be marked up in correct AsciiDoc-format. 

## Modules
The documentation is structures so that each module correspond to a chapter in the manual. Every module should contain the following folders: 
- assets/ Here are e.g. images stored 
- pages/ Here are all the AsciiDoc files located. 
- examples/ Here is source code for examples located. 

Every module also has a nav.docs-file that describes the pages hierarchy to use for proper display in a side-menu.

## antora.yml
This file is the component descriptor which act as a component marker to signal that this is an documentation component. It also specifies where to find the module navigation files. 