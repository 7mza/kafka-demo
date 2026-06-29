package com.hamza.kafka.commons

import org.hibernate.boot.registry.StandardServiceInitiator
import org.hibernate.boot.registry.StandardServiceRegistryBuilder
import org.hibernate.bytecode.internal.none.BytecodeProviderImpl
import org.hibernate.bytecode.spi.BytecodeProvider
import org.hibernate.service.spi.ServiceContributor
import org.hibernate.service.spi.ServiceRegistryImplementor

/*
 * bytebuddy runtime class generation don't work in native-image
 * this forces a no-op provider when GraalVm is detected
 * it's done via the StandardServiceRegistryBuilder.addInitiator SPI instead of ServiceLoader
 * because latter break hibernate enhancement plugin
 *
 * FIXME: this will not work with lazy associations
 *  need to use no proxy enhancement based laziness instead of normal proxy
 *  example: @LazyToOne(LazyToOneOption.NO_PROXY)
 */
class NoneBytecodeProviderInitiator : StandardServiceInitiator<BytecodeProvider> {
    override fun getServiceInitiated() = BytecodeProvider::class.java

    override fun initiateService(
        configurationValues: Map<String, Any>,
        registry: ServiceRegistryImplementor,
    ) = BytecodeProviderImpl()
}

class NativeBytecodeProviderContributor : ServiceContributor {
    override fun contribute(serviceRegistryBuilder: StandardServiceRegistryBuilder) {
        if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
            serviceRegistryBuilder.addInitiator(NoneBytecodeProviderInitiator())
        }
    }
}
