#!/usr/bin/env python
# ----------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------

"""Generate pipeline (default: gpbackup-generated.yml) from template (default:
templates/gpbackup-tpl.yml).

Python module requirements:
  - jinja2 (install through pip or easy_install)
"""

import argparse
import datetime
import glob
import os
import re
import subprocess
import yaml

from jinja2 import Environment, FileSystemLoader

# raw_input isn't defined in Python3.x, whereas input wasn't behaving like raw_input in Python 2.x
# this should make both input and raw_input work in Python 2.x/3.x like the raw_input from Python 2.x
try: input = raw_input
except NameError: raw_input = input

PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))

TEMPLATE_ENVIRONMENT = Environment(
    autoescape=False,
    loader=FileSystemLoader(os.path.join(PIPELINES_DIR, 'templates')),
    trim_blocks=True,
    lstrip_blocks=True,
    variable_start_string='[[', # 'default {{ has conflict with pipeline syntax'
    variable_end_string=']]',
    extensions=['jinja2.ext.loopcontrols']
)

def suggested_git_branch():
    """Try to guess the current git branch"""
    branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode('utf-8').rstrip()
    return branch

def render_template(template_filename, context):
    """Render pipeline template yaml"""
    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(context)


def create_pipeline(args):
    context = {
        'template_filename': args.template_filename,
        'generator_filename': os.path.basename(__file__),
        'timestamp': datetime.datetime.now(),
        'pipeline_name': args.pipeline_name,
        'nightly_trigger': args.nightly_trigger,
        'is_prod': args.is_prod,
        'should_alert': args.should_alert
    }

    pipeline_yml = render_template(args.template_filename, context)

    if args.is_prod:
        default_output_filename = "%s-generated.yml" % args.pipeline_name
        pipeline_yml = pipeline_yml.replace("$$DEV_PROD$$", "prod")
    else:
        default_output_filename = "%s-dev-generated.yml" % args.pipeline_name
        pipeline_yml = pipeline_yml.replace("$$DEV_PROD$$", "dev")

    with open(default_output_filename, 'w') as output:
        header = render_template('pipeline_header.yml', context)
        output.write(header)
        output.write(pipeline_yml)

    return True

def print_output_message(args):
    git_branch = suggested_git_branch()
    curr_dir = os.getcwd()
    if not args.is_prod:
        if git_branch == "main":
            print("\n[WARNING] You are generating a dev pipeline pointed to the main branch!\n")
        cmd = """fly -t dp set-pipeline -p %s \
-c %s/%s-dev-generated.yml \
-v gpbackup-git-branch=%s""" % (git_branch, curr_dir, args.pipeline_name, git_branch)
        print("To set this pipeline on dev, run: \n%s" % (cmd))
        join = raw_input('Would you like to run the pipeline now? [yN]: ')
        if join.lower() == 'yes' or join.lower() == 'y':
            # Expand all home directory paths (i.e. ~/workspace...)
            cmd = [os.path.expanduser(p) if p[0] == '~' else p for p in cmd.replace('\\\n', '').split()]
            subprocess.call(cmd)
        else:
            print("bailing out")

    if args.is_prod:
        if git_branch != "main":
            print("\n[WARNING] You are generating a prod pipeline, but are not on the main branch!\n")
        cmd1 = "fly -t dp set-pipeline -p %s \
-c %s/%s-generated.yml \
-v gpbackup-git-branch=%s" % (args.pipeline_name, curr_dir, args.pipeline_name, git_branch)
        args.pipeline_name = "gpbackup"
        cmd2 = "fly -t dp set-pipeline -p %s \
-c %s/%s-generated.yml \
-v gpbackup-git-branch=%s" % (args.pipeline_name, curr_dir, args.pipeline_name, git_branch)
        print("To set these pipelines (gpbackup / gpbackup-release) on prod, run: \n%s\n%s" % (cmd2, cmd1))

def main():
    """main: parse args and create pipeline"""
    parser = argparse.ArgumentParser(
        description='Generate Concourse Pipeline utility',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-T',
        '--template',
        action='store',
        dest='template_filename',
        default="gpbackup-tpl.yml",
        help='Name of template to use, in templates/'
    )

    parser.add_argument(
        '-na',
        '--no-alerts',
        action='store_false',
        dest='should_alert',
        default=True,
        help='Disable the Slack alert for failed jobs'
    )

    parser.add_argument(
        '-nt',
        '--nightly-trigger',
        action='store_true',
        dest='nightly_trigger',
        default=False,
        help='Set nightly triggers. Only applies to gpbackup'
    )

    parser.add_argument(
        '-p',
        '--pipeline-name',
        action='store',
        dest='pipeline_name',
        default='gpbackup',
        help='Specify the pipeline config you would like to generate: {gpbackup, gpbackup-release}'
    )

    parser.add_argument(
        '--prod',
        action='store_true',
        dest='is_prod',
        default=False,
        help='Set if the pipeline to be generated is for prod'
    )

    args = parser.parse_args()

    # NOTE: The nightly trigger is enabled for all prod pipelines
    if args.is_prod:
        args.nightly_trigger = True 

    pipeline_created = create_pipeline(args)

    if args.is_prod:
        args.pipeline_name = "gpbackup-release"
        pipeline_created = create_pipeline(args)

    if not pipeline_created:
        exit(1)

    print_output_message(args)


if __name__ == "__main__":
    main()
