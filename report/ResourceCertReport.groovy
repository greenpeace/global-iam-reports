import org.apache.commons.collections.CollectionUtils
import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.searchbeans.ResourceSearchBean
import org.openiam.idm.searchbeans.ResourceTypeSearchBean
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.grp.service.GroupDataService
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.res.dto.Resource
import org.openiam.idm.srvc.res.dto.ResourceType
import org.openiam.idm.srvc.res.service.ResourceDataService
import org.openiam.idm.srvc.role.service.RoleDataService
import org.openiam.idm.srvc.user.dto.User
import org.openiam.idm.srvc.user.ws.UserDataWebService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.DateFormat
import java.text.ParseException

public class ResourceCertReport implements ReportDataSetBuilder {

    final static String DEFAULT_REQUESTER_ID = "3000"
    final static Language DEFAULT_LANGUAGE = new Language(id:  1)
    final static String NOT_FOUND = '[ Not found ]'

    def masterTableKeys = new HashSet<String>()

    private ApplicationContext context
    private UserDataWebService userWebService
    private AuditLogService auditLogService
    private ResourceDataService resourceService
    private RoleDataService roleService
    private GroupDataService groupService
    private OrganizationDataService organizationService

    DateFormat dateFormat

    @Override
    public ReportDataDto getReportData(ReportQueryDto query) {

        println "ResourceCertReport data source, request: " + query.queryParams.values().join(", ")
        userWebService = context.getBean("userWS") as UserDataWebService
        auditLogService = context.getBean(AuditLogService.class)
        resourceService = context.getBean(ResourceDataService.class)
        roleService = context.getBean(RoleDataService.class)
        groupService = context.getBean(GroupDataService.class)
        organizationService = context.getBean(OrganizationDataService.class)

        def String ownerId = query.getParameterValue("OWNER_ID")
        def String resTypeId = query.getParameterValue("RES_TYPE_ID")
        def List<String> resIds = query.getParameterValues("RES_IDS")
        def String periodStartString = query.getParameterValue("PERIOD_START")
        def String periodEndString = query.getParameterValue("PERIOD_END")
        def Date periodStart = dateFromString(periodStartString)
        def Date periodEnd = dateFromString(periodEndString)

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"

        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.name = "head"
            if (ownerId) {
                row.column.add(new ReportColumn('OWNER', getUserFullName(ownerId)))
            }
            if (!EmptyMultiValue(resIds)) {

                def targetName = ""
                resIds.each { resId ->
                    def resName = findResource(resId)?.name
                    if (resName) {
                        targetName += (targetName ? ", " : "") + resName
                    }
                }
                row.column.add(new ReportColumn('RESOURCE', targetName ?: NOT_FOUND))
            }
            if (resTypeId) {
                def resType = findResourceType(resTypeId)
                row.column.add(new ReportColumn('RES_TYPE', resType?.name ?: NOT_FOUND))
            }
            if (periodStart) {
                def period = "from " + dateToString(periodStart)
                row.column.add(new ReportColumn('PERIOD_START', periodStart.time.toString()))
                if (periodEnd) {
                    period += " to " + dateToString(periodEnd)
                    row.column.add(new ReportColumn('PERIOD_END', periodEnd.time.toString()))
                }
                row.column.add(new ReportColumn('PERIOD', period))
            }
            reportTable.row.add(row)

        } else {

            reportTable.name = "details"
            def messages = validateParameters(ownerId, resTypeId, resIds, periodStart, periodEnd) as String[]
            if (messages) {
                for(def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                //def assocBean = resId ? findResource(resId) : null

                def AuditLogSearchBean logSearchBean = new AuditLogSearchBean()
                logSearchBean.action = "COMPLETE_WORKFLOW"
                if (periodStart)
                    logSearchBean.from = periodStart
                if (periodEnd)
                    logSearchBean.to = periodEnd
                if (ownerId)
                    logSearchBean.userId = ownerId
                logSearchBean.sortBy = [new SortParam(OrderConstants.DESC, "timestamp")] as List
                def auditLogs = auditLogService.findBeans(logSearchBean, 0, 10000)
                def isResFilter = !EmptyMultiValue(resIds)
                for(IdmAuditLog l : auditLogs) {
                    def log = auditLogService.findById(l.id)
                    def parent = log.parentLogs.find({ it.action == "resourceCertification" })
                    if (parent) {
                        parent = auditLogService.findById(parent.id)
                        def isConfirmFilter = !isResFilter && !resTypeId
                        if (!isConfirmFilter) {
                            if (isResFilter) {
                                isConfirmFilter = parent.customRecords.find {
                                    it.key == "AssociationId" && resIds.contains(it.value)
                                }
                            } else if (resTypeId) {
                                def assoccRecs = parent.customRecords.findAll { it.key == "AssociationId" }
                                isConfirmFilter = assoccRecs && findResource(assoccRecs.value, resTypeId)
                            }
                        }
                        if (isConfirmFilter) {
                            addMainTableRow(parent, log, reportTable)
                        }
                    }
                }

                println "ResourceCertReport data source, response size: " + reportTable.row.size() + " rows"
            }
        }

        return new ReportDataDto( tables : [ reportTable ] as List<ReportTable> )
    }

    private void addMainTableRow(IdmAuditLog parent, IdmAuditLog log, ReportTable reportTable) {

        final String ownerId = log.userId
        final String employeeId = parent.customRecords.find({it.key == "MemberAssociationId"})?.value
        final String assocId = parent.customRecords.find({it.key == "AssociationId"})?.value
        final String assocType = parent.customRecords.find({it.key == "AssociationType"})?.value
        final String key = employeeId + "," + ownerId + "," + assocId

        if (assocId) {
            if (!masterTableKeys.contains(key)) {
                masterTableKeys.add(key)

                def employeeName = getUserFullName(employeeId)
                def ownerName = getUserFullName(ownerId)
                def assocName = getAssociationName(assocId, assocType)
                def date = dateToString(log.timestamp)
                def isTaskApproved = log.customRecords.find({it.key == "IsTaskApproved"})?.value

                ReportRow row = new ReportRow()
                row.column.add(new ReportColumn('EMPLOYEE', employeeName))
                row.column.add(new ReportColumn('OWNER', ownerName))
                row.column.add(new ReportColumn('RESOURCE', assocName))
                row.column.add(new ReportColumn('LAST_DATE', date))
                row.column.add(new ReportColumn('APPROVED', isTaskApproved ? (isTaskApproved == "true" ? "Y" : "N") : " "))
                reportTable.row.add(row)
            }
        }
    }

    static def validateParameters(String ownerId, String resTypeId, List<String> resIds, Date periodStart, Date periodEnd) {
        def violations = [] as List
        if (!resTypeId && !ownerId && EmptyMultiValue(resIds))
            violations.add "Parameter 'Resource type' or 'Resource' or 'Resource owner' is required"
        if (!periodStart)
            violations.add "Parameter 'Period start' is required"
        if (periodStart && periodEnd && periodEnd < periodStart)
            violations.add "Parameter 'Period start' has to be less than 'Period end'"
        return violations
    }

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    def String getUserFullName(String userId) {
        def User user = userWebService.getUserWithDependent(userId, DEFAULT_REQUESTER_ID, false)
        return user ? (user.firstName +
                (user.middleInit ? ' ' + user.middleInit : '') +
                (user.lastName ? ' ' + user.lastName : '')) : NOT_FOUND;
    }

    def String getAssociationName(String id, String type) {
        if (type == "RESOURCE") {
            def res = resourceService.getResource(id, DEFAULT_LANGUAGE)
            if (res) {
                return "[" + res.resourceType?.displayName + "] " +
                        (res.displayName ?: res.coorelatedName ?: res.name)
            }
        }
        return NOT_FOUND
    }

    def KeyNameBean findResource(String resId) {
        // search resource
        def resourceSearchBean = new ResourceSearchBean( key: resId )
        def resources = resourceService.findBeans(resourceSearchBean, 0, 1, DEFAULT_LANGUAGE) as List<Resource>
        if (resources) {
            def res = resources.get(0)
            def displayName = "[" + res.resourceType?.displayName + "] " +
                    (res.displayName ?: res.coorelatedName ?: res.name)

            return new KeyNameBean(id: res.id, name: displayName)
        }
        return null
    }

    def KeyNameBean findResource(Collection<String> resIds, String resTypeId) {
        for(String resId : resIds) {
            def resourceSearchBean = new ResourceSearchBean(key: resId, resourceTypeId: resTypeId)
            def resources = resourceService.findBeans(resourceSearchBean, 0, 1, DEFAULT_LANGUAGE) as List<Resource>
            if (resources) {
                def res = resources.get(0)
                return new KeyNameBean(id: res.id, name: res.displayName)
            }
        }
        return null
    }

    def KeyNameBean findResourceType(String resTypeId) {
        def searchBean = new ResourceTypeSearchBean( key: resTypeId )
        def types = resourceService.findResourceTypes(searchBean, 0, 1, DEFAULT_LANGUAGE) as List<ResourceType>
        if (types) {
            def type = types.get(0)
            return new KeyNameBean(id: type.id, name: type.displayName ?: type.id)
        }
        return null
    }

    private class KeyNameBean { def String id; def String name; }

    private static boolean EmptyMultiValue(List<String> values) {
        return !values || (values.size() == 1 && !values[0])
    }

    Date dateFromString(String periodStartString) {
        try {
            return dateFormat && periodStartString ? dateFormat.parse(periodStartString) : null
        } catch (ParseException ignored) {
            return null
        }
    }

    String dateToString(Date date) {
        return (date && dateFormat) ? dateFormat.format(date) : ""
    }
}
