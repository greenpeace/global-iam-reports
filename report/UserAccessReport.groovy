package reports

import org.openiam.authmanager.common.SetStringResponse
import org.openiam.authmanager.model.UserEntitlementsMatrix
import org.openiam.authmanager.service.AuthorizationManagerAdminService
import org.openiam.idm.searchbeans.ResourceSearchBean
import org.openiam.idm.searchbeans.UserSearchBean
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.grp.dto.Group
import org.openiam.idm.srvc.grp.service.GroupDataService
import org.openiam.idm.srvc.lang.domain.LanguageEntity
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.mngsys.dto.ManagedSysDto
import org.openiam.idm.srvc.mngsys.service.ManagedSystemService
import org.openiam.idm.srvc.mngsys.ws.ManagedSystemWebService
import org.openiam.idm.srvc.org.dto.Organization
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.res.domain.ResourceEntity
import org.openiam.idm.srvc.res.dto.ResourceRisk
import org.openiam.idm.srvc.res.service.ResourceService
import org.openiam.idm.srvc.role.dto.Role
import org.openiam.idm.srvc.role.service.RoleDataService
import org.openiam.idm.srvc.user.domain.UserEntity
import org.openiam.idm.srvc.user.service.UserDataService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

public class UserAccessReport implements ReportDataSetBuilder {

    final static String NOT_FOUND = '[ Not found ]'
    final static int USERS_LIMIT = 10000
    final static int RECURSION_LIMIT = 10
    final static Language DEFAULT_LANGUAGE = new Language(id: 1)
    final static LanguageEntity DEFAULT_LANGUAGE_ENTITY = new LanguageEntity(id: 1)
    final static String DIRECT_RESOURCES = 'Directly assigned'
    final static String CONTAINER_GROUP = 'Group'
    final static String CONTAINER_ROLE = 'Role'

    private def role2ResCache = new HashMap<String, Set<String>>()
    private def group2ResCache = new HashMap<String, Set<String>>()
    private def resCache = new HashMap<String, ResBean>()

    private ApplicationContext context
    private UserDataService userDataService
    private OrganizationDataService organizationService
    private RoleDataService roleService
    private GroupDataService groupService
    private ResourceService resourceService
    private AuditLogService auditLogService
    private ManagedSystemService managedSystemService
    private AuthorizationManagerAdminService authManagerService

    private Long startTime = 0
    private int PROCESSING_TIMEOUT = 290

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    @Override
    ReportDataDto getReportData(ReportQueryDto query) {
        return processDataRequest(query)
    }

    ReportDataDto processDataRequest(query) {

        println ">>> User access report, request: " + query.queryParams.values().join(", ")

        startTime = new Date().time
        userDataService = context.getBean("userManager") as UserDataService
        organizationService = context.getBean("orgManager") as OrganizationDataService
        roleService = context.getBean(RoleDataService.class)
        groupService = context.getBean(GroupDataService.class)
        auditLogService = context.getBean(AuditLogService.class)
        managedSystemService = context.getBean(ManagedSystemService.class)
        authManagerService = context.getBean(AuthorizationManagerAdminService.class)
        resourceService = context.getBean(ResourceService.class)

        def String orgId = query.getParameterValue("ORG_ID")
        def String roleId = query.getParameterValue("ROLE_ID")
        def String groupId = query.getParameterValue("GROUP_ID")
        def String userId = query.getParameterValue("USER_ID")
        def String risk = query.getParameterValue("RISK")?.toUpperCase()
        def String manSysId = query.getParameterValue("MANAGED_SYS_ID")
        def String[] resTypeIds = query.getParameterValues("RES_TYPE_IDS")
        def String[] resIds = query.getParameterValues("RESOURCE_IDS")

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"
        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.name = "head"

            if (orgId) {
                def Organization bean = organizationService.getOrganizationLocalized(orgId, null, DEFAULT_LANGUAGE)
                row.column.add(new ReportColumn('ORGANIZATION', bean?.name ?: NOT_FOUND))
            }
            if (roleId) {
                def Role bean = roleService.getRoleDTO(roleId)
                row.column.add(new ReportColumn('ROLE', bean?.name ?: NOT_FOUND))
            }
            if (groupId) {
                def Group bean = groupService.getGroupDTO(groupId)
                row.column.add(new ReportColumn('GROUP', bean?.name ?: NOT_FOUND))
            }
            if (userId) {
                def searchBean = new UserSearchBean(deepCopy: false, key: userId)
                def users = userDataService.findBeans(searchBean)
                if (users) {
                    def user = users.get(0)
                    def fullName = user.userAttributes.get("FULL_NAME")?.value ?:
                            user.firstName + ' ' + (user.middleInit ? user.middleInit + ' ' : '') + user.lastName
                    row.column.add(new ReportColumn('USER', user ? fullName : NOT_FOUND))
                }
            }
            if (risk) {
                row.column.add(new ReportColumn('RISK', risk?.toUpperCase()))
            }
            if (manSysId) {
                def manSystem = managedSystemService.getManagedSysById(manSysId)
                row.column.add(new ReportColumn('MANAGED_SYSTEM', manSystem?.name ?: NOT_FOUND))
            }
            if (!EmptyMultiValue(resTypeIds)) {
                def names = ""
                resTypeIds.each { resTypeId ->
                    def name = resourceService.findResourceTypeById(resTypeId)?.description
                    if (name) {
                        names += (names ? ", " : "") + name
                    }
                }
                row.column.add(new ReportColumn('RES_TYPES', names ?: NOT_FOUND))
            }
            if (!EmptyMultiValue(resIds)) {
                def names = ""
                resourceService.findResourcesByIds(Arrays.asList(resIds))?.each { re ->
                    def name = re.coorelatedName ?: re.name
                    names += (names ? ", " : "") + name
                }
                row.column.add(new ReportColumn('RESOURCES', names ?: NOT_FOUND))
            }
            reportTable.row.add(row)

        } else {

            reportTable.setName("details")

            def messages = validateParameters(orgId, roleId, groupId, userId, risk, manSysId, resTypeIds, resIds) as String[]
            if (messages) {
                for(def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                // Select root resources
                HashSet<String> resIdsSet = null
                if (!EmptyMultiValue(resIds)) {
                    resIdsSet = new HashSet<String>(Arrays.asList(resIds))
                } else if (manSysId) {

                    def managedSystem = managedSystemService.getManagedSysById(manSysId)
                    resIdsSet = [:] as HashSet<String>
                    resIdsSet += managedSystem.resourceId
                }

                if (!resIdsSet) {
                    // Find resources by risk and type
                    if (risk || !EmptyMultiValue(resTypeIds)) {
                        resIdsSet = [:] as HashSet<String>
                        if (!EmptyMultiValue(resTypeIds)) {
                            resTypeIds.each { typeId ->
                                def searchBean = new ResourceSearchBean(deepCopy: false, risk: risk ?: null, resourceTypeId: typeId)
                                def resources = resourceService.findBeans(searchBean, 0, Integer.MAX_VALUE)
                                resIdsSet += resources?.id
                            }
                        } else {
                            def searchBean = new ResourceSearchBean(deepCopy: false, risk: risk)
                            def resources = resourceService.findBeans(searchBean, 0, Integer.MAX_VALUE)
                            resIdsSet += resources?.id
                        }
                    }
                } else if (manSysId) {
                    // Select child resources
                    def resources = [:] as HashSet<ResourceEntity>
                    resources += resourceService.findResourcesByIds(resIdsSet)

                    def parentIds = resIdsSet
                    def iteration = 0
                    while (++iteration <= RECURSION_LIMIT) {
                        def searchBean = new ResourceSearchBean(deepCopy: false, parentIdSet: parentIds)
                        def children = resourceService.findBeans(searchBean, 0, Integer.MAX_VALUE)
                        if (!children) break
                        resources += children
                        parentIds = children.id as Set<String>
                    }

                    def resRisk = risk ? ResourceRisk.getByValue(risk) : null
                    def typeIds = EmptyMultiValue(resTypeIds) ? null : new HashSet(Arrays.asList(resTypeIds))
                    resIdsSet = resources.findAll({ ResourceEntity re ->
                        return ((!resRisk || re.risk == resRisk) && (!typeIds || typeIds.contains(re.resourceType.id)))
                    })?.id
                }

                boolean isUserFilterDefined = userId || roleId || groupId || orgId

                def searchBean = new UserSearchBean(deepCopy: false)
                def users = [] as List<UserEntity>

                if (isUserFilterDefined) {

                    // Select users
                    if (userId) {
                        searchBean.key = userId
                    } else {
                        if (orgId) searchBean.organizationIdSet = [orgId] as Set
                        if (roleId) searchBean.roleIdSet = [roleId] as Set
                        if (groupId) searchBean.groupIdSet = [groupId] as Set
                    }
                    users = userDataService.findBeans(searchBean, 0, USERS_LIMIT)
                }

                if (resIdsSet && (!isUserFilterDefined || (resIdsSet.size()<100 && users.size()>100))) {
                    def userIdsMap = authManagerService.getUserIdsEntitledForResourceSet(resIdsSet) as HashMap<String, SetStringResponse>
                    def userIdsSet = [:] as HashSet<String>
                    userIdsMap.each { entry ->
                        if (entry?.value?.setString) {
                            userIdsSet.addAll(entry.value.setString)
                        }
                    }

                    if (!isUserFilterDefined) {
                        userIdsSet.each { id ->
                            searchBean.key = id
                            def foundUser = userDataService.findBeans(searchBean, 0, 1)
                            if (foundUser) {
                                users += foundUser.get(0)
                            }
                        }
                    } else {
                        users = users.findAll { u -> userIdsSet.contains(u.id) }
                    }
                }
                if (users || resIdsSet) {
                    println ">>> User access report: building details, users: " + (users?.size() ?: '?') +
                            ", resources: " + (resIdsSet?.size() ?: '?')
                    populateDetailRows(users, resIdsSet, reportTable)
                }
            }
        }
        println ">>> User access report - complete"
        return packReportTable(reportTable)
    }

    private void populateDetailRows(List<UserEntity> users, Set<String> resIds, ReportTable reportTable) {

        def total = users?.size() ?: 0
        def counter = 0
        for(UserEntity user : users) {

            if (counter % 30 == 0) {
                def duration = ((new Date().time - startTime) / 1000) as int
                println ">>> User access report: $counter users processed. Duration: $duration sec"
                if (PROCESSING_TIMEOUT <= duration) {
                    println ">>> User access report: Processing timeout reached: $PROCESSING_TIMEOUT sec. $counter of $total users processed."
                    break
                }
            }

            // retrieve resources entitled to the user
            def entitlementBeans = getUserEntitlementBeans(user.id, resIds)
            entitlementBeans.each { eb ->
                eb.resources.each { res ->
                    def row = new ReportRow()
                    row.column.add(new ReportColumn('FULL_NAME', user.userAttributes.get("FULL_NAME")?.value ?:
                            user.firstName + ' ' + (user.middleInit ? user.middleInit + ' ' : '') + user.lastName))
                    row.column.add(new ReportColumn('EMPLOYEE_ID', user.employeeId))
                    row.column.add(new ReportColumn('CONTAINER_TYPE', eb.type))
                    row.column.add(new ReportColumn('CONTAINER_NAME', chunkWords(eb.name)))
                    row.column.add(new ReportColumn('RES_NAME', chunkWords(res.name)))
                    row.column.add(new ReportColumn('RES_TYPE', res.typeName))
                    row.column.add(new ReportColumn('RES_DESC', chunkWords(res.desc)))
                    row.column.add(new ReportColumn('RISK', res.risk))
                    reportTable.row.add(row)
                }
            }
            ++counter
        }
        if (counter < total) {
            // add warning
            def row = new ReportRow()
            def msg = "Processing timeout reached. " +
                      "Users processed: $counter of $total. " +
                      "Define stricter parameters to get complete report" as String
            row.column.add(new ReportColumn('WARNING', msg))
            reportTable.row.add(row)
        }
    }

    def validateParameters(String orgId, String roleId, String groupId, String userId, String risk, String manSysId,
                                  String[] resTypeIds, String[] resIds) {
        def violations = [] as List
        if (userId && (orgId || roleId || groupId))
            violations.add "Parameters 'Organization', 'Group' and 'Role' are ignored when 'User' is specified"
        if (!EmptyMultiValue(resIds) && manSysId) {
            violations.add "Parameter 'Managed system' is ignored when 'Resource' is specified"
        }
        if (manSysId) {
            def managedSystem = managedSystemService.getManagedSysById(manSysId)
            if (!managedSystem?.resourceId) {
                violations.add "Parameter 'Managed system' has invalid value"
            }
        }
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

    static String chunkWords(String line) {
        return line && line.length() > 10 ? line.replaceAll(/_(\S)/, '_ $1') : line
    }

    List<EntitlementBean> getUserEntitlementBeans(String userId, Set<String> resIdsFilter) {

        def matrix = authManagerService.getUserEntitlementsMatrix(userId)
        def result = [] as List<EntitlementBean>
        if (matrix.resourceIds) {
            def resBeans = getResourceBeans(matrix.resourceIds, resIdsFilter)
            if (resBeans) {
                def directResources = new EntitlementBean(type: DIRECT_RESOURCES)
                directResources.resources += resBeans
                result += directResources
            }
        }

        matrix.roleIds.each { roleId ->
            def resourceIds = getResourcesForRole(roleId, matrix)
            if (resourceIds) {
                def resBeans = getResourceBeans(resourceIds, resIdsFilter)
                if (resBeans) {
                    def roleBean = getRoleBean(roleId)
                    roleBean.resources += resBeans
                    result += roleBean
                }
            }
        }

        matrix.groupIds.each { groupId ->
            def resourceIds = getResourcesForGroup(groupId, matrix)
            if (resourceIds) {
                def resBeans = getResourceBeans(resourceIds, resIdsFilter)
                if (resBeans) {
                    def groupBean = getGroupBean(groupId)
                    groupBean.resources += resBeans
                    result += groupBean
                }
            }
        }

        return result
    }

    Set<String> getResourcesForRole(String roleId, UserEntitlementsMatrix matrix){
        if(!role2ResCache.containsKey(roleId)) {
            def compiledRoles = compileTreeBranch(roleId, matrix.childRoleToParentRoleMap, [] as HashSet)
            compiledRoles += roleId
            def resourcesForRoles = joinCompiledBlocks(compiledRoles, matrix.roleToResourceMap)
            Set<String> allResources = resourcesForRoles + joinTreeBlocks(resourcesForRoles, matrix.childResToParentResMap)
            role2ResCache.put(roleId, allResources)
        }
        return role2ResCache.get(roleId)
    }

    Set<String> getResourcesForGroup(String groupId, UserEntitlementsMatrix matrix){
        if(!group2ResCache.containsKey(groupId)) {
            def compiledRoles = [] as HashSet
            matrix.groupToRoleMap.get(groupId).each { String roleId ->
                compiledRoles += roleId
                compiledRoles += compileTreeBranch(roleId, matrix.childRoleToParentRoleMap, [] as HashSet)
            }
            def compiledGroups = compileTreeBranch(groupId, matrix.childGroupToParentGroupMap, [] as HashSet)
            compiledGroups += groupId

            Set<String> resultIds = joinCompiledBlocks(compiledGroups, matrix.groupToResourceMap)
            compiledRoles += joinCompiledBlocks(compiledGroups, matrix.groupToRoleMap)
            resultIds += joinCompiledBlocks(compiledRoles, matrix.roleToResourceMap)
            resultIds += joinTreeBlocks(resultIds, matrix.childResToParentResMap)
            group2ResCache.put(groupId, resultIds)
        }
        return group2ResCache.get(groupId)
    }

    List<ResBean> getResourceBeans(Set<String> resIds, Set<String> resIdsFilter) {
        def resBeans = [] as List<ResBean>
        def notCachedIds = [] as List<String>
        def includeMenu = resIdsFilter as boolean

        resIds.each { resId ->
            if (!resIdsFilter || resIdsFilter.contains(resId)) {
                def res = resCache.get(resId)//getResourceBean(resId)
                if (res) {
                    if (includeMenu || res.typeId != 'MENU_ITEM') {
                        resBeans += res
                    }
                } else {
                    notCachedIds += resId
                }
            }
        }
        // Build new beans for not cached resources
        def resources = resourceService.findResourcesByIds(notCachedIds)
        resources.each { resEntity ->
            def resBean = new ResBean(
                    id: resEntity.id,
                    name: resEntity.coorelatedName ?: resEntity.name,
                    risk: resEntity.risk,
                    typeId: resEntity.resourceType.id,
                    desc: resEntity.description,
                    typeName: resEntity.resourceType.displayNameMap?.get(DEFAULT_LANGUAGE.id)?.value)
            resCache.put(resEntity.id, resBean)
            if (includeMenu || resBean.typeId != 'MENU_ITEM') {
                resBeans += resBean
            }
        }
        return resBeans
    }

    EntitlementBean getRoleBean(String roleId) {
        def role = roleService.getRole(roleId)
        return new EntitlementBean(
                name: role?.name ?: NOT_FOUND,
                type: CONTAINER_ROLE)
    }

    EntitlementBean getGroupBean(String groupId) {
        def group = groupService.getGroup(groupId)
        return new EntitlementBean(
                name: group?.name ?: NOT_FOUND,
                type: CONTAINER_GROUP)
    }

    static Set<String> joinCompiledBlocks(def blockIds, def compiledMap) {
        def resultIds = [] as HashSet
        blockIds.each { blockId ->
            if (compiledMap.get(blockId)) {
                resultIds += compiledMap.get(blockId)
            }
        }
        return resultIds
    }

    static Set<String> joinTreeBlocks(def blockIds, def treeMap) {
        def resultIds = [] as HashSet
        blockIds.each { final String blockId ->
            resultIds += compileTreeBranch(blockId, treeMap, [] as HashSet)
        }
        return resultIds;
    }

    static Set<String> compileTreeBranch(String branchId, def treeMap, def visitedIds) {
        def resultIds = [] as HashSet
        if(!(branchId in visitedIds)) {
            visitedIds += branchId
            treeMap.get(branchId).each { String childId ->
                resultIds += childId
                resultIds += compileTreeBranch(childId, treeMap, visitedIds)
            }
        }
        return resultIds
    }

    private static boolean EmptyMultiValue(String[] values) {
        return !values || (values.length == 1 && !values[0])
    }

    static class EntitlementBean {
        // A name declared with no access modifier generates a private field with public getter and setter
        String type
        String name
        Set<ResBean> resources = [] as Set<ResBean>
    }

    static class ResBean {
        String id
        String typeId
        String typeName
        String name
        String risk
        String desc
    }

}
