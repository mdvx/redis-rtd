# redis-rtd
Excel RTD server sourcing data from Redis


## Installation
1. Clone the repository and go to its folder.
2. Compile the code using Visual Studio, MSBuild or via this handy script file:

   `build.cmd`


3. Register the COM server by running the following script in admin command prompt:
   
   `register.cmd`

## Usage

Once the RTD server has been installed, you can use it from Excel via the RTD macro.
This is the syntax:

`=RTD("redis",, "HOST","TBA", "TBA")`


<!-- ![Excel screenshot](doc/redis-rtd-excel.png) -->

