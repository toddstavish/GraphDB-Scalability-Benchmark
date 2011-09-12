package bench;

import com.infinitegraph.BaseVertex;

class Topic extends BaseVertex
{
    private String topic;

    public Topic(String topic)
    {
        setTopic(topic);
    }

    public void setTopic(String topic)
    {
        markModified();
        this.topic = topic;
    }

    public String getTopic()
    {
        fetch();
        return this.topic;
    }

}