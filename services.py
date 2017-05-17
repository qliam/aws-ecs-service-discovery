#!/usr/bin/env python
"""
A toolkit for identifying and advertising service resources.

Uses a specific naming convention for the Task Definition of services.  If you
name the Task Definition ending with "-service", no configuration is needed.
This also requires that you not use that naming convention for task definitions
that are not services.

For example:
    A Task Definition with the family name of 'cache-service' will have its
    hosting Container Instance's internal ip added to a Route53 private Zone as
    cache.local and other machines on the same subnet can address it that way.
"""

import argparse
import logging
import os
import re
import boto3
import sys

ecs = boto3.client('ecs', region_name='eu-west-1')
ec2 = boto3.client('ec2', region_name='eu-west-1')
route53 = boto3.client('route53', region_name='eu-west-1')

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y/%m/%d/ %I:%M:%S %p')
logging.getLogger().setLevel(logging.INFO)
log = logging.info
cluster = 'pumpkin-docker-cluster-prod'

sys.stdout.write('cluster identified as: {0}'.format(cluster))

def get_task_arns():
    """Request all API pages needed to get Task ARNS."""
    next_token = ''
    arns = []
    while next_token is not None:
        detail = ecs.list_tasks(cluster=cluster, desiredStatus='RUNNING', nextToken=next_token)
        arns.extend(detail['taskArns'])
        if 'nextToken' in detail:
          next_token = detail['nextToken']
        else:
          next_token = None
    return arns

def get_service_arns():
    """Request all API pages needed to get Service ARNS."""
    next_token = ''
    arns = []
    while next_token is not None:
        detail = ecs.list_services(cluster=cluster, nextToken=next_token)
        arns.extend(detail['serviceArns'])
        if 'nextToken' in detail:
          next_token = detail['nextToken']
        else:
          next_token = None
    return arns

def get_primary_tasks_for_service(service_arn):
    """Get the task ARN of the primary service"""
    response = ecs.describe_services(cluster=cluster, services=[service_arn])
    for deployment in response['services'][0]['deployments']:
        if deployment['status'] == 'PRIMARY':
            return get_tasks_for_task_definition(deployment['taskDefinition'])
    return None

def get_tasks_for_task_definition(task_definition):
    """Get the ARN of running task, given the task definition."""
    next_token = ''
    arns = []
    tasks = []

    task_arns = get_task_arns()
    response = ecs.describe_tasks(cluster=cluster, tasks=task_arns)
    for task in response['tasks']:
        if task['taskDefinitionArn'] == task_definition:
            tasks.append(task)
    return tasks

def get_container_instance_ec2_id(container_instance_arn):
    """Id the EC2 instance serving as the container instance."""
    detail = ecs.describe_container_instances(
        cluster=cluster,
        containerInstances=[container_instance_arn.encode('UTF-8')])
    return detail['containerInstances'][0]['ec2InstanceId'].encode('UTF-8')


def get_ec2_instance(ec2_instance_id):
    """Get the primary interface for the given EC2 instance."""
    filter = [{'Name': 'instance-id', 'Values': [ec2_instance_id]}]
    instances = ec2.describe_instances(Filters=filter)
    return instances['Reservations'][0]['Instances'][0]['NetworkInterfaces'][0]


def get_zone_for_vpc(vpc_id):
    """Identify the Hosted Zone for the given VPC.

    Assumes a 1 to 1 relationship.

    NOTE: There is an existing bug.
    https://github.com/boto/boto/issues/3061
    When that changes, I expect to have to search ['VPCs'] as a list of
    dictionaries rather than a dictionary.  This has the unfortunate side
    effect of not working for Hosted Zones that are associated with more than
    one VPC. (But, why would you expect internal DNS for 2 different private
    networks to be the same anyway?)
    """
    response = route53.list_hosted_zones()
    for zone in response['HostedZones']:
        zone_id = zone['Id']#.split('/')[-1]
        detail = route53.get_hosted_zone(Id=zone_id)
        if 'VPCs' in detail and detail['VPCs'][0]['VPCId'] == vpc_id:
            return {'zone_id': zone_id, 'zone_name': zone['Name']}


def get_info():
    """Get all needed info about running services.

    WARNING: locals() is used to assemble the returned dictionary. Any
    variables defined in this function must begine with _ to be kept out of the
    return value.
    """
    _info = {'services': [], 'network': {'cluster': cluster}}
    _service_arns = get_service_arns()
    for service_arn in _service_arns:
        service = ecs.describe_services(cluster=cluster, services=[service_arn])['services'][0]
        name = service['serviceName']
        log('{name} service found'.format(**locals()))

        tasks = get_primary_tasks_for_service(service_arn)
        container_instance_private_ips = set()
        if tasks:
            for task in tasks:
                task_arn = task['taskArn']
                log('  {task_arn} is PRIMARY'.format(**locals()))

                container_instance_arn = task['containerInstanceArn']
                ec2_instance_id = get_container_instance_ec2_id(container_instance_arn)
                ec2_instance = get_ec2_instance(ec2_instance_id)
                if ec2_instance:
                    container_instance_private_ips.add(ec2_instance['PrivateIpAddress'])

            _services = {k: v for (k, v) in locals().iteritems() if k[0] != '_'}
            _info['services'].append(_services)
            # No need to get common network info on each loop over tasks
            if 'vpc_id' not in _info['network']:
                _info['network'].update(get_zone_for_vpc(ec2_instance['VpcId']))
                _info['network']['vpc_id'] = ec2_instance['VpcId']
    return _info


def dns(zone_id, zone_name, service_name, service_ips, ttl=20):
    """Insert or update DNS record."""

    resourceRecords = map(lambda ip: { 'Value': ip }, service_ips)

    record = {
      'Comment': 'string',
      'Changes': [
        {
          'Action': 'UPSERT',
          'ResourceRecordSet': {
            'Name': '{service_name}.{zone_name}'.format(**locals()),
            'Type': 'A',
            'TTL': ttl,
            'ResourceRecords': resourceRecords
          }
        }
      ]
    }

    rrs = route53.change_resource_record_sets(HostedZoneId=zone_id, ChangeBatch=record)
    return rrs


def update_services(service_names=[], verbose=False):
    """Update DNS to allow discovery of properly named task definitions.

    If service_names are provided only update those services.
    Otherwise update all.
    """
    info = get_info()
    for service in info['services']:
        if (service_names and
                service['family'] not in service_names and
                service['name'] not in service_names):
            continue
        if verbose:
            log('Registering {0}.{1} as {2}'.format(
                service['name'], info['network']['zone_name'],
                ', '.join(service['container_instance_private_ips'])))
        dns(info['network']['zone_id'], info['network']['zone_name'],
            service['name'], service['container_instance_private_ips'])


def cli():
    """Used by entry_point console_scripts."""
    parser = argparse.ArgumentParser()
    parser.add_argument('service_names', nargs='*',
                        help='list of services to start')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='supress output')
    args = parser.parse_args()
    if not args.quiet:
        logging.getLogger().setLevel(logging.INFO)
    update_services(args.service_names, True)

pattern_arn = re.compile(
    'arn:'
    '(?P<partition>[^:]+):'
    '(?P<service>[^:]+):'
    '(?P<region>[^:]*):'   # region is optional
    '(?P<account>[^:]*):'  # account is optional
    '(?P<resourcetype>[^:/]+)([:/])'
    '(?P<resource>('
        '(?P<family>[^:]+):'     # noqa
        '(?P<version>[^:]+)|.*'  # noqa
    '))')

if __name__ == '__main__':
    cli()
