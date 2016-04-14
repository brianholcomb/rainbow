import time
import json
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError


class StackStatus(str):
    pass


class StackSuccessStatus(StackStatus):
    pass


class StackFailStatus(StackStatus):
    pass


class CloudformationException(Exception):
    pass


class Cloudformation(object):
    # this is from http://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_Stack.html
    # boto.cloudformation.stack.StackEvent.valid_states doesn't have the full list.
    VALID_STACK_STATUSES = ['CREATE_IN_PROGRESS', 'CREATE_FAILED', 'CREATE_COMPLETE', 'ROLLBACK_IN_PROGRESS',
                            'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE', 'DELETE_IN_PROGRESS', 'DELETE_FAILED',
                            'DELETE_COMPLETE', 'UPDATE_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                            'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_ROLLBACK_FAILED',
                            'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_ROLLBACK_COMPLETE']

    default_region = 'us-east-1'

    def __init__(self, region=None):
        """
        :param region: AWS region
        :type region: str
        """

        self.client = boto3.client('cloudformation', region_name=region or Cloudformation.default_region)

        try:
            self.client.describe_account_limits()
        except EndpointConnectionError:
            raise CloudformationException('Invalid region %s' % (self.client.meta.region_name))

    @staticmethod
    def resolve_template_parameters(template, datasource_collection):
        """
        Resolve all template parameters from datasource_collection, return a dictionary of parameters
        to pass to update_stack() or create_stack() methods

        :type template: dict
        :type datasource_collection: DataSourceCollection
        :rtype: dict
        :return: parameters parameter for update_stack() or create_stack()
        """

        parameters = []
        for parameter, parameter_definition in template.get('Parameters', {}).iteritems():
            if 'Default' in parameter_definition and not parameter in datasource_collection:
                parameter_value = parameter_definition['Default']
            else:
                parameter_value = datasource_collection.get_parameter_recursive(parameter)

                if hasattr(parameter_value, '__iter__'):
                    parameter_value = ','.join(map(str, parameter_value))

            parameters.append({'ParameterKey': str(parameter), 'ParameterValue': str(parameter_value)})

        return parameters

    def stack_exists(self, name):
        """
        Check if a CFN stack exists

        :param name: stack name
        :return: True/False
        :rtype: bool
        """

        stacks = []
        # conserve bandwidth (and API calls) by not listing any stacks in DELETE_COMPLETE state
        responses = self.client.get_paginator('list_stacks').paginate(
            StackStatusFilter=[status for status in Cloudformation.VALID_STACK_STATUSES if status != 'DELETE_COMPLETE'])
        try:
            for response in responses:
                stacks+=[stack['StackName'] for stack in response['StackSummaries']]
        except ClientError as ex:
            raise CloudformationException(ex.message)
        return name in stacks

    def update_stack(self, name, template, parameters):
        """
        Update CFN stack

        :param name: stack name
        :type name: str
        :param template: JSON encodeable object
        :type template: str
        :param parameters: dictionary containing key value pairs as CFN parameters
        :type parameters: dict
        :rtype: bool
        :return: False if there aren't any updates to be performed, True if no exception has been thrown.
        """

        try:
            self.client.update_stack(StackName=name, TemplateBody=json.dumps(template),
                                         Parameters=parameters, Capabilities=['CAPABILITY_IAM'])
        except ClientError as ex:
            if ex.message == 'No updates are to be performed.':
                # this is not really an error, but there aren't any updates.
                return False
            else:
                raise CloudformationException(ex.message)
        else:
            return True

    def create_stack(self, name, template, parameters):
        """
        Create CFN stack

        :param name: stack name
        :type name: str
        :param template: JSON encodeable object
        :type template: str
        :param parameters: dictionary containing key value pairs as CFN parameters
        :type parameters: dict
        """

        try:
            self.client.create_stack(StackName=name, TemplateBody=json.dumps(template), DisableRollback=True,
                                         Parameters=parameters, Capabilities=['CAPABILITY_IAM'])
        except ClientError as ex:
            raise CloudformationException('error occured while creating stack %s: %s' % (name, ex.message))

    def describe_stack_events(self, name):
        """
        Describe CFN stack events

        :param name: stack name
        :type name: str
        :return: stack events
        :rtype: list of boto.cloudformation.stack.StackEvent
        """

        events = []
        responses = self.client.get_paginator('describe_stack_events').paginate(StackName=name)
        try:
            for response in responses:
                events+=[event for event in response['StackEvents']]
        except ClientError as ex:
            raise CloudformationException(ex.message)

        return events

    def get_stack(self, name):
        """
        Describe CFN stack

        :param name: stack name
        :return: stack object
        :rtype: boto.cloudformation.stack.Stack
        """

        try:
            return boto3.resource('cloudformation', region_name=self.client.meta.region_name).Stack(name)
        except ClientError as ex:
            raise CloudformationException(ex.message)

    def tail_stack_events(self, name, initial_entry=None):
        """
        This function is a wrapper around _tail_stack_events(), because a generator function doesn't run any code
        before the first iterator item is accessed (aka .next() is called).
        This function can be called without an `inital_entry` and tail the stack events from the bottom.

        Each iteration returns either:
        1. StackFailStatus object which indicates the stack creation/update failed (last iteration)
        2. StackSuccessStatus object which indicates the stack creation/update succeeded (last iteration)
        3. dictionary describing the stack event, containing the following keys: resource_type, logical_resource_id,
           physical_resource_id, resource_status, resource_status_reason

        A common usage pattern would be to call tail_stack_events('stack') prior to running update_stack() on it,
        thus creating the iterator prior to the actual beginning of the update. Then, after initiating the update
        process, for loop through the iterator receiving the generated events and status updates.

        :param name: stack name
        :type name: str
        :param initial_entry: where to start tailing from. None means to start from the last item (exclusive)
        :type initial_entry: None or int
        :return: generator object yielding stack events
        :rtype: generator
        """

        if initial_entry is None:
            return self._tail_stack_events(name, len(self.describe_stack_events(name)))
        elif initial_entry < 0:
            return self._tail_stack_events(name, len(self.describe_stack_events(name)) + initial_entry)
        else:
            return self._tail_stack_events(name, initial_entry)

    def _tail_stack_events(self, name, initial_entry):
        """
        See tail_stack_events()
        """

        previous_stack_events = initial_entry

        while True:
            stack = self.client.describe_stacks(StackName=name)['Stacks'][0]
            stack_events = self.describe_stack_events(name)

            if len(stack_events) > previous_stack_events:
                # iterate on all new events, at reversed order (the list is sorted from newest to oldest)
                for event in stack_events[:-previous_stack_events or None][::-1]:
                    yield {'resource_type': event.get('ResourceType'),
                           'logical_resource_id': event.get('LogicalResourceId'),
                           'physical_resource_id': event.get('PhysicalResourceId'),
                           'resource_status': event.get('ResourceStatus'),
                           'resource_status_reason': event.get('ResourceStatusReason'),
                           'timestamp': event.get('Timestamp')}

                previous_stack_events = len(stack_events)

            if stack['StackStatus'].endswith('_FAILED') or \
                    stack['StackStatus'] in ('ROLLBACK_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'):
                yield StackFailStatus(stack['StackStatus'])
                break
            elif stack['StackStatus'].endswith('_COMPLETE'):
                yield StackSuccessStatus(stack['StackStatus'])
                break

            time.sleep(5)
