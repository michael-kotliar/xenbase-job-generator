import sys
import argparse
import os
import json
import collections


RNA_SEQ_SE_PE = ('{{'
                  '"sra_input_file": {{"class": "File", "location": "{sra_input_file}", "format": "http://edamontology.org/format_3698"}},'                  
                  '"illumina_adapters_file": {{"class": "File", "location": "{illumina_adapters_file}", "format": "http://edamontology.org/format_1929"}},'
                  '"rsem_indices_folder": {{"class": "Directory", "location": "{rsem_indices_folder}"}},'
                  '"chr_length_file": {{"class": "File", "location": "{chr_length_file}", "format": "http://edamontology.org/format_2330"}},'
                  '"threads": {threads}'
                '}}')

CHIP_SEQ_SE_PE = ('{{'
                  '"sra_input_file": {{"class": "File", "location": "{sra_input_file}", "format": "http://edamontology.org/format_3698"}},'                  
                  '"illumina_adapters_file": {{"class": "File", "location": "{illumina_adapters_file}", "format": "http://edamontology.org/format_1929"}},'
                  '"bowtie2_indices_folder": {{"class": "Directory", "location": "{bowtie2_indices_folder}"}},'
                  '"chr_length_file": {{"class": "File", "location": "{chr_length_file}", "format": "http://edamontology.org/format_2330"}},'
                  '"threads": {threads}'
                '}}')

WORKFLOW_PREFIX = {
    'CHIP-SE': 'xenbase-chipseq-se-',
    'CHIP-PE': 'xenbase-chipseq-pe-',
    'RNA-SE':  'xenbase-rnaseq-se-',
    'RNA-PE':  'xenbase-rnaseq-pe-'
}


def normalize(args):
    """Converts all relative path arguments to absolute ones relatively to the current working directory"""
    normalized_args = {}
    for key,value in args.__dict__.iteritems():
        if key in ['folder','adapter','indices','crhlength','output']:
            normalized_args[key] = value if not value or os.path.isabs(value) else os.path.normpath(os.path.join(os.getcwd(), value))
        else:
            normalized_args[key]=value
    return argparse.Namespace (**normalized_args)


def get_abs_file_list(folder):
    return {os.path.splitext(filename)[0]: os.path.normpath(os.path.join(folder, filename)) for filename in os.listdir(folder)}


def generate_jobs (args, filelist):
    template_job = CHIP_SEQ_SE_PE if args.type in ['CHIP-SE', 'CHIP-PE'] else RNA_SEQ_SE_PE
    jobs = {}
    for file_name, file_path in filelist.iteritems():
        kwargs = {
            "sra_input_file": file_path,
            "illumina_adapters_file": args.adapter,
            "rsem_indices_folder": args.indices,
            "bowtie2_indices_folder": args.indices,
            "chr_length_file": args.crhlength,
            "threads": args.threads
        }
        filled_job_object = json.loads(template_job.format(**kwargs).replace("'True'",'true').replace("'False'",'false').replace('"True"','true').replace('"False"','false'))
        filled_job_object['uid'] = file_name;
        jobs[file_name] = json.dumps(collections.OrderedDict(sorted(filled_job_object.items())), indent=4)
    return jobs

def export_jobs(args, jobs):
    for job_name, job_data in jobs.iteritems():
        output_filename = os.path.join(args.output, WORKFLOW_PREFIX[args.type]+job_name+'.json')
        if os.path.isfile(output_filename):
            print "Duplicate job file name, skipped", output_filename
            continue
        with open(output_filename, 'w') as output_file:
            output_file.write(job_data)


def arg_parser():
    general_parser = argparse.ArgumentParser()
    general_parser.add_argument("-t", "--type",      help="Workflow type: CHIP-SE|CHIP-PE|RNA-SE|RNA-PE", choices=['RNA-SE','RNA-PE','CHIP-SE','CHIP-PE'], required=True)
    general_parser.add_argument("-f", "--folder",    help="Path to SRA files", required=True)
    general_parser.add_argument("-a", "--adapter",   help="Path to adapters file", required=True)
    general_parser.add_argument("-i", "--indices",   help="Path to indices folder", required=True)
    general_parser.add_argument("-c", "--crhlength", help="Path to chrom length file", required=True)
    general_parser.add_argument("-o", "--output",    help="Output folder for generated jobs", default='.')
    general_parser.add_argument("-m", "--threads", help="Number of threads to use", default=1)
    return general_parser


def main(argsl=None):
    if argsl is None:
        argsl = sys.argv[1:]
    args,_ = arg_parser().parse_known_args(argsl)
    try:
        args = normalize(args)
        abs_file_list = get_abs_file_list(args.folder)
        jobs = generate_jobs(args, abs_file_list)
        export_jobs (args, jobs)
    except Exception as ex:
        print "Something went wrong", ex.str()
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))