#!/bin/bash

export PROJECTS_DIR=$HOME/projects
export OPENCV_DIR=$HOME/projects/opencv
export OPENCV_BUILD_DIR=$OPENCV_DIR/build

install_base_rpms()
{
  sudo apt-get install -y xfsprogs
  sudo apt-get install -y python-pip
  sudo apt-get install -y python
  sudo apt-get install -y cmake
  sudo apt-get install -y python-devel numpy
  sudo apt-get install -y gcc gcc-c++
  sudo apt-get install -y gtk2-devel
  sudo apt-get install -y libdc1394-devel
  sudo apt-get install -y libv4l-devel
  sudo apt-get install -y gstreamer-plugins-base-devel
  sudo apt-get install -y libpng-devel libjpeg-turbo-devel jasper-devel openexr-devel libtiff-devel libwebp-devel
  sudo apt-get install -y tbb-devel
  sudo apt-get install -y eigen3-devel
  sudo apt-get install -y python-sphinx texlive
}

install_ffmpeg()
{
  sudo apt-get install ffmpeg
}

setup_opencv()
{

 cd $PROJECTS_DIR

 git clone git@github.com:opencv/opencv.git
 git clone git@github.com:opencv/opencv_contrib.git

 mkdir -p $OPENCV_BUID_DIR
}

build_opencv()
{
 cd $OPENCV_BUILD_DIR
 cmake -D CMAKE_BUILD_TYPE=RELEASE -D CMAKE_INSTALL_PREFIX=/usr/local ..

#Threading Building Blocks (TBB) and Eigen (Optimized mathematical Operations)
#Several OpenCV functions are parallelized with Intel's Threading Building Blocks (TBB). But if you want to enable it,
#you need to install TBB first. ( Also while configuring installation with CMake,
#don't forget to pass -D WITH_TBB=ON. More details below.)
#OpenCV uses another library Eigen for optimized mathematical operations. So if you have Eigen installed in your system,
#you can exploit it. ( Also while configuring installation with CMake, don't forget to pass -D WITH_EIGEN=ON.
#More details below.)
cmake -D WITH_TBB=ON -D WITH_EIGEN=ON ..

#Disable GPU related modules
#Since we use OpenCV-Python, we don't need GPU related modules. It saves us some time
cmake -D WITH_OPENCL=OFF -D WITH_CUDA=OFF -D BUILD_opencv_gpu=OFF -D BUILD_opencv_gpuarithm=OFF -D BUILD_opencv_gpubgsegm=OFF -D BUILD_opencv_gpucodec=OFF -D BUILD_opencv_gpufeatures2d=OFF -D BUILD_opencv_gpufilters=OFF -D BUILD_opencv_gpuimgproc=OFF -D BUILD_opencv_gpulegacy=OFF -D BUILD_opencv_gpuoptflow=OFF -D BUILD_opencv_gpustereo=OFF -D BUILD_opencv_gpuwarping=OFF ..

#Run the following command for displaying the Output
cmake -D CMAKE_BUILD_TYPE=RELEASE -D CMAKE_INSTALL_PREFIX=/usr/local ..
}

#install_base_rpms

#install_ffmpeg

build_opencv
