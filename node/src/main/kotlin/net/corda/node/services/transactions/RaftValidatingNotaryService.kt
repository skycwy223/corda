package net.corda.node.services.transactions

import net.corda.core.flows.FlowLogic
import net.corda.core.identity.Party
import net.corda.core.node.services.TimeWindowChecker
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.config.FullNodeConfiguration

/** A validating notary service operated by a group of mutually trusting parties, uses the Raft algorithm to achieve consensus. */
class RaftValidatingNotaryService(val timeWindowChecker: TimeWindowChecker,
                                  val uniquenessProvider: RaftUniquenessProvider) : NotaryService() {
    companion object {
        val type = ValidatingNotaryService.type.getSubType("raft")
    }

    object Factory : NotaryService.Factory<RaftValidatingNotaryService> {
        override fun create(services: ServiceHubInternal, serializationContext: MutableList<Any>): RaftValidatingNotaryService {
            val config = services.configuration as FullNodeConfiguration
            val timeWindowChecker = TimeWindowChecker(services.clock)
            val uniquenessProvider = RaftUniquenessProvider(
                    config.baseDirectory,
                    config.notaryNodeAddress!!,
                    config.notaryClusterAddresses,
                    services.database,
                    config
            )
            serializationContext.add(uniquenessProvider)
            return RaftValidatingNotaryService(timeWindowChecker, uniquenessProvider)
        }
    }

    override val serviceFlowFactory: (Party, Int) -> FlowLogic<Void?> = { otherParty, _ ->
        ValidatingNotaryFlow(otherParty, timeWindowChecker, uniquenessProvider)
    }

    override fun start() {
        uniquenessProvider.start()
    }

    override fun stop() {
        uniquenessProvider.stop()
    }
}
