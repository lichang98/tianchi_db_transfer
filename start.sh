#!/bin/sh

make
echo "start"
case $1 in
	--input_dir)
		in_dir=$2
		;;
	--output_dir)
		out_dir=$2
		;;
esac

case $3 in
	--input_dir)
		in_dir=$4
		;;
	--output_dir)
		out_dir=$4
		;;
esac

mkdir ${out_dir}"/sink_file_dir"
./transfer ${in_dir} ${out_dir}
echo "end"
exit 0
