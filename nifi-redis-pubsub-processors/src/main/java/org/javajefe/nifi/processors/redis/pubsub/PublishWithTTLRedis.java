package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.javajefe.nifi.processors.redis.pubsub.util.RedisAction;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Alexander Bukarev on 14.11.2018.
 */
//@SupportsBatching
@SuppressWarnings("WeakerAccess")
@SeeAlso({ SubscribeRedis.class })
@Tags({ "Redis", "PubSub", "Queue" })
@CapabilityDescription("PUBLISH, LPUSH, RPUSH commands support to emulate a queue using Redis.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PublishWithTTLRedis extends AbstractRedisProcessor {

    public static final AllowableValue PUBLISH_MODE = new AllowableValue("PUBLISH");
    public static final AllowableValue LPUSH_MODE = new AllowableValue("LPUSH");
    public static final AllowableValue RPUSH_MODE = new AllowableValue("RPUSH");
    public static final PropertyDescriptor QUEUE_MODE = new PropertyDescriptor.Builder()
            .name("Queue Mode")
            .description("Queue implementation mode (Pub/Sub or List implementation)")
            .required(true)
            .allowableValues(PUBLISH_MODE, LPUSH_MODE, RPUSH_MODE)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CHANNEL_OR_LIST = new PropertyDescriptor.Builder()
        .name("Channel/Key Name")
        .description("Channel name (for PUBLISH mode) or Key name the list is stored (for LPUSH and RPUSH modes)")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();


    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
        .name("Key TTL")
        .description("xPush TTL value")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("60")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        final List<PropertyDescriptor> descriptors = new ArrayList<>(this.descriptors);
        descriptors.add(TTL);
        this.descriptors = Collections.unmodifiableList(descriptors);
    }
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile flowFile = session.get();

        // Send each FlowFile to Redis
        byte[] channelOrKeyBytes = context.getProperty(CHANNEL_OR_LIST).evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8);
        Long ttl = Long.valueOf(context.getProperty(TTL).evaluateAttributeExpressions(flowFile).getValue());
        executePublish(session, channelOrKeyBytes, flowFile, ttl);
	}

	private byte[] serializeFlowFile(ProcessSession session, FlowFile flowFile) {
        byte[] result;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        result = baos.toByteArray();
        return result;
    }

	private void executePublish(ProcessSession session, byte[] channelOrKeyBytes, FlowFile flowFile, Long ttl) {
        final long start = System.nanoTime();
        String channelOrKeyCompiled = new String(channelOrKeyBytes);
        byte[] flowFileContent = serializeFlowFile(session, flowFile);
        try {
            final RedisAction<Void> action = redisConnection -> {
                switch (mode) {
                    case "PUBLISH":
                        redisConnection.publish(channelOrKeyBytes, flowFileContent);
                        break;
                    case "LPUSH":
                        redisConnection.listCommands().lPush(channelOrKeyBytes, new byte[][]{flowFileContent});
                        break;
                    case "RPUSH":
                        redisConnection.listCommands().rPush(channelOrKeyBytes, new byte[][]{flowFileContent});
                        break;
                    default:
                        throw new UnsupportedOperationException("Queue mode " + mode + " is not supported");
                }
                redisConnection.expire(channelOrKeyBytes, ttl);
                return null;
            };
            withConnection(action);
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            getLogger().debug("Successfully published {} message to Redis '{}' for {} with {}", new Object[]{mode, channelOrKeyCompiled, ttl, flowFile});
            session.getProvenanceReporter().send(flowFile, channelOrKeyCompiled, transmissionMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception ex) {
            getLogger().info("Failed to publish message to Redis for {}", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

	@SuppressWarnings("unused")
    @OnScheduled
    public void startPublishing(final ProcessContext context) {
        redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL)
                .asControllerService(RedisConnectionPool.class);
        mode = context.getProperty(QUEUE_MODE).getValue();
    }

    @Override
    protected PropertyDescriptor getQueueModePropertyDescriptor() {
        return QUEUE_MODE;
    }

    @Override
    protected PropertyDescriptor getChannelOrKeyPropertyDescriptor() {
        return CHANNEL_OR_LIST;
    }
}
