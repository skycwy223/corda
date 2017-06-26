package net.corda.node.services.transactions

import net.corda.core.flows.FlowLogic
import net.corda.core.identity.Party
import net.corda.core.node.services.ServiceType
import net.corda.core.node.services.TimeWindowChecker
import net.corda.core.node.services.UniquenessProvider
import net.corda.node.services.api.ServiceHubInternal

/** A simple Notary service that does not perform transaction validation */
class SimpleNotaryService(val timeWindowChecker: TimeWindowChecker,
                          val uniquenessProvider: UniquenessProvider) : NotaryService() {
    companion object {
        val type = ServiceType.notary.getSubType("simple")
    }

    object Factory : NotaryService.Factory<SimpleNotaryService> {
        override fun create(services: ServiceHubInternal, serializationContext: MutableList<Any>): SimpleNotaryService {
            val uniquenessProvider = PersistentUniquenessProvider()
            serializationContext.add(uniquenessProvider)
            return SimpleNotaryService(TimeWindowChecker(services.clock), uniquenessProvider)
        }
    }

    override val serviceFlowFactory: (Party, Int) -> FlowLogic<Void?> = { otherParty, _ ->
        NonValidatingNotaryFlow(otherParty, timeWindowChecker, uniquenessProvider)
    }
}