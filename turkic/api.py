from datetime import datetime
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("turkic.api")
CommunicationError = ClientError


class Server(object):
    def __init__(self, signature, accesskey, localhost, sandbox=False):
        self.localhost = localhost

        if sandbox:
            url = "https://mturk-requester-sandbox.us-east-1.amazonaws.com"
        else:
            url = "https://mturk-requester.us-east-1.amazonaws.com"

        self.client = boto3.client('mturk', endpoint_url=url, region_name='us-east-1',
                                   aws_access_key_id=accesskey, aws_secret_access_key=signature)

    def createhit(self, title, description, page, amount, duration,
                  lifetime, keywords="", autoapprove=604800, height=650,
                  minapprovedpercent=None, minapprovedamount=None,
                  countrycode=None):
        """
        Creates a HIT on Mechanical Turk.

        If successful, returns a Response object that has fields:
            hit_id          The HIT ID
            hit_type_id     The HIT group ID

        If unsuccessful, a CommunicationError is raised with a message
        describing the failure.
        """
        r = {"Title": title,
             "Description": description,
             "Keywords": keywords,
             "Reward": '%0.2f' % amount,
             "AssignmentDurationInSeconds": duration,
             "AutoApprovalDelayInSeconds": autoapprove,
             "LifetimeInSeconds": lifetime}

        qualifications = []

        if minapprovedpercent:
            qualifications.append({
                "QualificationTypeId": "000000000000000000L0",
                "Comparator": "GreaterThanOrEqualTo",
                "IntegerValue": minapprovedpercent
            })

        if minapprovedamount:
            qualifications.append({
                "QualificationTypeId": "00000000000000000040",
                "Comparator": "GreaterThanOrEqualTo",
                "IntegerValue": minapprovedamount
            })

        if countrycode:
            qualifications.append({
                "QualificationTypeId": "00000000000000000071",
                "Comparator": "EqualTo",
                "IntegerValue": countrycode
            })

        r["Question"] = ("<ExternalQuestion xmlns=\"http://mechanicalturk"
                         ".amazonaws.com/AWSMechanicalTurkDataSchemas/"
                         "2006-07-14/ExternalQuestion.xsd\">"
                         "<ExternalURL>{0}/{1}</ExternalURL>"
                         "<FrameHeight>{2}</FrameHeight>"
                         "</ExternalQuestion>").format(self.localhost,
                                                       page, height)

        r = self.client.create_hit(**r)
        return r

    def disable(self, hitid):
        """
        Disables the HIT from the MTurk service.
        """
        return self.client.update_expiration_for_hit(
            HITId=hitid, ExpireAt=datetime.utcnow())

    def purge(self):
        """
        Disables all the HITs on the MTurk server. Useful for debugging.
        """
        raise Exception("You probably don't want to do this")

    def accept(self, assignmentid, feedback=""):
        """
        Accepts the assignment and pays the worker.
        """
        return self.client.approve_assignment(
            AssignmentId=assignmentid,
            RequesterFeedback=feedback
        )

    def reject(self, assignmentid, feedback=""):
        """
        Rejects the assignment and does not pay the worker.
        """
        return self.client.reject_assignment(
            AssignmentId=assignmentid,
            RequesterFeedback=feedback
        )

    def bonus(self, workerid, assignmentid, amount, feedback=""):
        """
        Grants a bonus to a worker for an assignment.
        """
        return self.client.reject_assignment(
            WorkerId=workerid,
            AssignmentId=assignmentid,
            BonusAmount='%0.2f' % amount,
            Reason=feedback
        )

    def block(self, workerid, reason=""):
        """
        Blocks the worker from working on any of our HITs.
        """
        return self.client.create_worker_block(
            WorkerId=workerid,
            Reason=reason
        )

    def unblock(self, workerid, reason=""):
        """
        Unblocks the worker and allows him to work for us again.
        """
        return self.client.delete_worker_block(
            WorkerId=workerid,
            Reason=reason
        )

    def email(self, workerid, subject, message):
        """
        Sends an email to the worker.
        """
        return self.client.notify_workers(
            WorkerIds=[workerid],
            Subject=subject,
            MessageText=message
        )

    def getstatistic(self, statistic, type, timeperiod="LifeToDate"):
        """
        Returns the total reward payout.
        """
        raise Exception("Not supported by new mturk api")

    @property
    def balance(self):
        """
        Returns a response object with the available balance in the amount
        attribute.
        """
        return float(self.client.get_account_balance()["AvailableBalance"])

    @property
    def rewardpayout(self):
        """
        Returns the total reward payout.
        """
        reward = self.getstatistic("TotalRewardPayout", float)
        bonus = self.getstatistic("TotalBonusPayout", float)
        return reward + bonus

    @property
    def approvalpercentage(self):
        """
        Returns the percent of assignments approved.
        """
        return self.getstatistic("PercentAssignmentsApproved", float)

    @property
    def feepayout(self):
        """
        Returns how much we paid to Amazon in fees.
        """
        reward = self.getstatistic("TotalRewardFeePayout", float)
        bonus = self.getstatistic("TotalBonusFeePayout", float)
        return reward + bonus

    @property
    def numcreated(self):
        """
        Returns the total number of HITs created.
        """
        return self.getstatistic("NumberHITsCreated", int)


try:
    import config
except ImportError:
    pass
else:
    server = Server(config.signature,
                    config.accesskey,
                    config.localhost,
                    config.sandbox)
