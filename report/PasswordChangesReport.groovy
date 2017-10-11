package org.openiam.connector

import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.searchbeans.UserSearchBean
import org.openiam.idm.srvc.audit.constant.AuditAction
import org.openiam.idm.srvc.audit.constant.AuditResult
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.org.dto.Organization
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.role.dto.Role
import org.openiam.idm.srvc.role.service.RoleDataService
import org.openiam.idm.srvc.user.service.UserDataService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat

public class PasswordChangesReport implements ReportDataSetBuilder {

    final static String NOT_FOUND = '[ Not found ]'
    final static int LOGS_LIMIT = 10000
    final static Language DEFAULT_LANGUAGE = new Language(id: 1)

    private ApplicationContext context
    private UserDataService userDataService
    private OrganizationDataService organizationService
    private RoleDataService roleService
    private AuditLogService auditLogService

    DateFormat dateFormat

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    @Override
    ReportDataDto getReportData(ReportQueryDto query) {

        println "PasswordChangesReport data source, request: " + query.queryParams.values().join(", ")
        userDataService = context.getBean("userManager") as UserDataService
        organizationService = context.getBean("orgManager") as OrganizationDataService
        roleService = context.getBean(RoleDataService.class)
        auditLogService = context.getBean(AuditLogService.class)

        def String orgId = query.getParameterValue("ORG_ID")
        def String periodStartString = query.getParameterValue("PERIOD_START")
        def String periodEndString = query.getParameterValue("PERIOD_END")
        def Date periodStart = dateFromString(periodStartString)
        def Date periodEnd = dateFromString(periodEndString)

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"
        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.name = "head"

            if (orgId) {
                def Organization bean = organizationService.getOrganizationLocalized(orgId, null, DEFAULT_LANGUAGE)
                row.column.add(new ReportColumn('ORGANIZATION', bean?.name ?: NOT_FOUND))
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

            reportTable.setName("details")

            def messages = validateParameters(orgId, periodStart, periodEnd) as String[]
            if (messages) {
                for (def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                def searchBean = new AuditLogSearchBean(
                        from: periodStart,
                        to: periodEnd,
                        action: AuditAction.USER_RESETPASSWORD.value(),
                        result: AuditResult.SUCCESS.value()
                )
                def logIds = auditLogService.findIDs(searchBean, 0, LOGS_LIMIT)
                def userResetStat = countLogsByTargetUser(logIds) as Map<String, Integer>
                searchBean.action = AuditAction.PROVISIONING_RESETPASSWORD.value()
                logIds = auditLogService.findIDs(searchBean, 0, LOGS_LIMIT)
                def provResetStat = countLogsByTargetUser(logIds) as Map<String, Integer>
                searchBean.action = AuditAction.CHANGE_PASSWORD.value()
                def logs = auditLogService.findBeans(searchBean, 0, LOGS_LIMIT)
                def changeStat = logs.countBy{it.userId}
                def allIds = userResetStat.keySet() + provResetStat.keySet() + changeStat.keySet()

                for(String userId : allIds) {
                    def user = userDataService.getUser(userId)
                    if (user) {
                        if (!orgId || user.organizationUser.find { it.organization?.id == orgId }) {
                            def row = new ReportRow()
                            row.column.add(new ReportColumn('FULL_NAME', user.userAttributes.get("FULL_NAME")?.value ?:
                                    user.firstName + ' ' + (user.middleInit ? user.middleInit + ' ' : '') + user.lastName))
                            row.column.add(new ReportColumn('TITLE', user.title))
                            row.column.add(new ReportColumn('EMPLOYEE_ID', user.employeeId))
                            row.column.add(new ReportColumn('CHANGES', (changeStat.get(userId) ?: 0).toString()))
                            def resets = (userResetStat.get(userId) ?: 0) + (provResetStat.get(userId) ?: 0)
                            row.column.add(new ReportColumn('RESETS', resets.toString()))
                            reportTable.row.add(row)
                        }
                    }
                }
            }
        }
        return packReportTable(reportTable)
    }

    def countLogsByTargetUser(List<String> logs) {
        def result = new HashMap<String, Integer>()
        logs.each {
            def log = auditLogService.findById(it)
            def userId = log.targets.find({it.targetType == "USER"})?.targetId
            result.put(userId, 1 + (result.get(userId) ?: 0))
        }
        return result
    }

    static def validateParameters(String orgId, Date periodStart, Date periodEnd) {
        def violations = [] as List
        if (!periodStart)
            violations.add "Parameter 'Period start' is required"
        if (periodStart && periodEnd && periodEnd < periodStart)
            violations.add "Parameter 'Period start' has to be less than 'Period end'"
        return violations
    }

    static ReportDataDto packReportTable(ReportTable reportTable)
    {
        ReportDataDto reportDataDto = new ReportDataDto()
        List<ReportTable> reportTables = new ArrayList<ReportTable>()
        reportTables.add(reportTable)
        reportDataDto.setTables(reportTables)
        return reportDataDto
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

