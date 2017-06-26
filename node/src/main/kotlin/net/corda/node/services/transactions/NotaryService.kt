package net.corda.node.services.transactions

import net.corda.core.flows.FlowLogic
import net.corda.core.identity.Party
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.node.services.api.ServiceHubInternal

abstract class NotaryService : SingletonSerializeAsToken() {

    interface Factory<out T : NotaryService> {
        fun create(services: ServiceHubInternal, serializationContext: MutableList<Any>): T
    }

    /**
     * Factory for producing notary service flows which have the corresponding sends and receives as NotaryFlow.Client.
     * The first parameter is the client [Party] making the request and the second is the platform version
     * of the client's node. Use this version parameter to provide backwards compatibility if the notary flow protocol
     * changes.
     */
    abstract val serviceFlowFactory: (Party, Int) -> FlowLogic<Void?>

    open fun start() {}
    open fun stop() {}
}