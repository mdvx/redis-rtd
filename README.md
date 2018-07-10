# redis-rtd
Excel RTD server sourcing data from Redis

![Excel screenshot](ice_video_20180709-212403.gif)


## Installation
1. Clone the repository and go to its folder.
2. Compile the code using Visual Studio, MSBuild or via this handy script file:

   `build.cmd`


3. Register the COM server by running the following script in admin command prompt:
   
   `register.cmd`

## Usage

Once the RTD server has been installed, you can use it from Excel via the RTD macro.
This is the syntax:

`=RTD("redis",, "HOST","CHANNEL")`

`=RTD("redis",, "HOST","CHANNEL", "FIELD")`   // For JSON data



